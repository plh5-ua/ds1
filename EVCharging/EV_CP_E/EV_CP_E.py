import asyncio, json, sys, random, threading
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

if len(sys.argv) < 3:
    print("Uso: python EV_CP_E.py <broker_ip:puerto> <puerto_engine>")
    sys.exit(1)

BROKER = sys.argv[1]
ENGINE_PORT = int(sys.argv[2])

CP_ID = None
CP_LOCATION = "Desconocida"
STATUS = "DESCONECTADO"
PRICE = round(random.uniform(0.40, 0.70), 2)
HEALTH = "OK"
STOP = asyncio.Event()
CHARGE_TASK = None


def control_thread():
    global HEALTH
    while True:
        try:
            cmd = input("").strip().upper()
        except EOFError:
            break
        if cmd == "KO":
            HEALTH = "KO"
            print("⚠️ Engine simulado en estado KO.")
        elif cmd == "OK":
            HEALTH = "OK"
            print("✅ Engine vuelve a estado OK.")


async def register_cp(producer: AIOKafkaProducer):
    msg = {"cp_id": CP_ID, "location": CP_LOCATION, "kwh": PRICE, "status": STATUS}
    await producer.send_and_wait("cp.register", json.dumps(msg).encode())
    print(f"📡 Registrando CP {CP_ID} en CENTRAL ({CP_LOCATION}) - {PRICE} €/kWh")


async def send_status(producer: AIOKafkaProducer, status: str):
    global STATUS
    STATUS = status
    payload = {"cp_id": CP_ID, "status": status}
    await producer.send_and_wait("cp.status", json.dumps(payload).encode())
    print(f"📡 Estado actualizado: {status}")


async def start_charging(producer: AIOKafkaProducer):
    """Simula una sesión de carga con aprobación manual al inicio y al final."""
    global STATUS, CHARGE_TASK

    # 🔸 Confirmar inicio manualmente
    confirm = await asyncio.to_thread(input, f"¿Aprobar inicio de carga en CP {CP_ID}? (s/n): ", flush=True)
    if confirm.lower() != "s":
        print("Carga no aprobada. Cancelando solicitud.")
        await send_status(producer, "PARADO")
        return

    STATUS = "SUMINISTRANDO"
    await send_status(producer, STATUS)
    print(f"Iniciando carga en {CP_ID} (Precio: {PRICE} €/kWh)")

    kwh = 0.0
    for _ in range(50):
        if STOP.is_set():
            break
        kw = round(random.uniform(6.0, 7.5), 2)
        kwh += kw / 3600 * 60 * 5
        amount = round(kwh * PRICE, 3)
        telem = {
            "cp_id": CP_ID,
            "session_id": 1,
            "kw": kw,
            "kwh_total": round(kwh, 3),
            "eur_total": amount
        }
        await producer.send_and_wait("cp.telemetry", json.dumps(telem).encode())
        print(f"🔋 {CP_ID}: {kw} kW — {amount:.3f} €")

        try:
            await asyncio.wait_for(STOP.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass

    # 🔸 Confirmar final manualmente antes de enviar fin de sesión
    print(f"¿Confirmar fin de carga en CP {CP_ID}? (s/n): ", flush=True)
    confirm_end = await asyncio.to_thread(input, "")

    if confirm_end.lower() != ("s" or "S"):
        print("⏸ Fin de carga no confirmado. Manteniendo sesión durante 10 segundos.")
        await asyncio.sleep(10)
        confirm_end = "s"

    end_msg = {
        "cp_id": CP_ID,
        "session_id": 1,
        "kwh": round(kwh, 3),
        "amount_eur": round(kwh * PRICE, 2),
        "reason": "ENDED" if not STOP.is_set() else "ABORTED"
    }
    await producer.send_and_wait("cp.session_ended", json.dumps(end_msg).encode())

    await send_status(producer, "PARADO" if STOP.is_set() else "ACTIVADO")
    print("✅ Carga finalizada.")
    CHARGE_TASK = None


async def handle_monitor_connection(reader, writer, producer):
    global CP_ID, CP_LOCATION, HEALTH
    try:
        data = await reader.read(4096)
        if not data:
            return

        message = data.decode().strip()

        if message == "PING":
            writer.write(b"OK"); await writer.drain(); return
        if message == "HEARTBEAT":
            writer.write(HEALTH.encode()); await writer.drain(); return

        try:
            payload = json.loads(message)
        except Exception:
            writer.write(b"NACK"); await writer.drain(); return

        if "cp_id" in payload and "location" in payload:
            CP_ID = payload["cp_id"]
            CP_LOCATION = payload["location"]
            print(f"✅ Recibido ID {CP_ID} y ubicación {CP_LOCATION}")
            writer.write(b"ACK"); await writer.drain()
            await send_status(producer, "ACTIVADO")
            return

        if "action" in payload:
            action = payload["action"].upper()
            print(f"⚙️ Orden local del monitor: {action}")

            if action == "PARAR":
                await send_status(producer, "PARADO")
                writer.write(b"OK"); await writer.drain(); return
            elif action == "ACTIVAR":
                await send_status(producer, "ACTIVADO")
                writer.write(b"OK"); await writer.drain(); return
            elif action == "SUMINISTRAR":
                await start_charging(producer)
                writer.write(b"OK"); await writer.drain(); return
            elif action == "KO":
                HEALTH = "KO"
                writer.write(b"OK"); await writer.drain(); return
            elif action == "OK":
                HEALTH = "OK"
                writer.write(b"OK"); await writer.drain(); return
            else:
                writer.write(b"NACK"); await writer.drain(); return

        writer.write(b"NACK"); await writer.drain()

    except Exception as e:
        print(f"⚠️ Error procesando mensaje del monitor: {e}")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def start_socket_server(producer):
    server = await asyncio.start_server(
        lambda r, w: handle_monitor_connection(r, w, producer),
        host="0.0.0.0",
        port=ENGINE_PORT
    )
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    print(f"🕐 Escuchando conexiones del monitor en {addrs}...")
    async with server:
        await server.serve_forever()


async def listen_to_central(consumer, producer):
    global CHARGE_TASK
    print("👂 Esperando mensajes de CENTRAL...")
    try:
        async for msg in consumer:
            data = msg.value

            if msg.topic == "central.authorize":
                if data.get("cp_id") != CP_ID:
                    continue
                if STOP.is_set() or STATUS != "ACTIVADO":
                    print("⛔ Ignoro authorize: CP no disponible.")
                    continue
                if STATUS == "SUMINISTRANDO" or (CHARGE_TASK and not CHARGE_TASK.done()):
                    print("ℹ️ Ya estaba suministrando; ignoro authorize duplicado.")
                    continue

                print("🔔 Autorizado suministro por CENTRAL (requiere aprobación manual)")
                CHARGE_TASK = asyncio.create_task(start_charging(producer))

            elif msg.topic == "central.command":
                action = data.get("action", "").upper()
                target = data.get("cp_id", "ALL")
                if target != "ALL" and target != CP_ID:
                    continue
                if action == "STOP":
                    STOP.set()
                    if CHARGE_TASK and not CHARGE_TASK.done():
                        pass
                    await send_status(producer, "PARADO")
                    print(f"🛑 Carga detenida por CENTRAL en {CP_ID}")
                elif action == "RESUME":
                    STOP.clear()
                    await send_status(producer, "ACTIVADO")
                    print(f"▶️ CP {CP_ID} reanudado por CENTRAL")

    except asyncio.CancelledError:
        pass


async def main():
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    consumer = AIOKafkaConsumer(
        "central.authorize", "central.command",
        bootstrap_servers=BROKER,
        value_deserializer=lambda b: json.loads(b.decode()),
        group_id=f"engine-{ENGINE_PORT}",
        auto_offset_reset="latest"
    )

    await producer.start()
    await consumer.start()
    print(f"🔌 EV_CP_E conectado a Kafka {BROKER}")
    print(f"🧩 Puerto del Engine para monitor: {ENGINE_PORT}")
    print("⌨️ Escribe 'KO' o 'OK' para simular el estado del engine.")

    threading.Thread(target=control_thread, daemon=True).start()

    socket_task = asyncio.create_task(start_socket_server(producer))
    central_task = asyncio.create_task(listen_to_central(consumer, producer))

    try:
        await asyncio.gather(socket_task, central_task)
    except KeyboardInterrupt:
        print("\n🛑 Interrumpido por teclado. Cerrando…")
    finally:
        STOP.set()
        for t in (socket_task, central_task):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        await consumer.stop()
        await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n🔴 Engine en puerto {ENGINE_PORT} detenido por el usuario.")
