import asyncio, json, sys, random, threading
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# -------------------------------------------------------------
# ARGUMENTOS
# -------------------------------------------------------------
if len(sys.argv) < 3:
    print("Uso: python EV_CP_E.py <broker_ip:puerto> <puerto_engine>")
    sys.exit(1)

BROKER = sys.argv[1]
ENGINE_PORT = int(sys.argv[2])

# -------------------------------------------------------------
# VARIABLES GLOBALES
# -------------------------------------------------------------
CP_ID = None
CP_LOCATION = "Desconocida"
STATUS = "DESCONECTADO"
PRICE = round(random.uniform(0.40, 0.70), 2)
HEALTH = "OK"  # para HEARTBEAT
STOP = asyncio.Event()  # señal de parada limpia
CHARGE_TASK = None  # asyncio.Task | None


# -------------------------------------------------------------
# CONTROL DESDE TECLADO (para simular KO/OK)
# -------------------------------------------------------------
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

# -------------------------------------------------------------
# KAFKA → CENTRAL
# -------------------------------------------------------------
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
    """Simula una sesión de carga y envía telemetrías periódicas a CENTRAL, cancelable con STOP."""
    global STATUS, CHARGE_TASK
    STATUS = "SUMINISTRANDO"
    await send_status(producer, STATUS)

    kwh = 0.0
    print(f"⚡ Iniciando carga en {CP_ID} (Precio: {PRICE} €/kWh)")

    for _ in range(10):
        if STOP.is_set():
            break
        kw = round(random.uniform(6.0, 7.5), 2)
        kwh += kw / 3600  * 60 * 10 #SIMULAMOS CARGA 10 MINUTOS POR SEGUNDO
        amount = round(kwh * PRICE, 3)
        telem = {
            "cp_id": CP_ID,
            "session_id": 1,           # ID fijo de demo
            "kw": kw,
            "kwh_total": round(kwh, 3),
            "eur_total": amount
        }
        await producer.send_and_wait("cp.telemetry", json.dumps(telem).encode())
        print(f"🔋 {CP_ID}: {kw} kW — {amount:.3f} €")

        # Espera 1s pero deja interrumpir con STOP
        try:
            await asyncio.wait_for(STOP.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass

    end_msg = {
        "cp_id": CP_ID,
        "session_id": 1,
        "kwh": round(kwh, 3),
        "amount_eur": round(kwh * PRICE, 2),
        "reason": "ENDED" if not STOP.is_set() else "ABORTED"
    }
    await producer.send_and_wait("cp.session_ended", json.dumps(end_msg).encode())

    # Si hemos abortado por STOP, el CP queda PARADO; si no, vuelve a ACTIVADO
    await send_status(producer, "PARADO" if STOP.is_set() else "ACTIVADO")
    print("✅ Carga finalizada.")
    CHARGE_TASK = None

# -------------------------------------------------------------
# SERVIDOR TCP ASÍNCRONO (Monitor ↔ Engine)
# -------------------------------------------------------------
async def handle_monitor_connection(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    producer: AIOKafkaProducer
):
    global CP_ID, CP_LOCATION, HEALTH
    try:
        data = await reader.read(4096)
        if not data:
            return

        message = data.decode().strip()

        # PING simple
        if message == "PING":
            writer.write(b"OK")
            await writer.drain()
            return

        # HEARTBEAT: devolvemos "OK"/"KO"
        if message == "HEARTBEAT":
            writer.write(HEALTH.encode())
            await writer.drain()
            return

        # Intentar JSON (registro o acción)
        try:
            payload = json.loads(message)
        except Exception:
            writer.write(b"NACK")
            await writer.drain()
            return

        # Registro inicial (ACK al monitor)
        if "cp_id" in payload and "location" in payload:
            CP_ID = payload["cp_id"]
            CP_LOCATION = payload["location"]
            print(f"✅ Recibido ID {CP_ID} y ubicación {CP_LOCATION}")
            writer.write(b"ACK")
            await writer.drain()

            await send_status(producer, "ACTIVADO")
            return
        # Acciones del monitor
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

        # JSON sin campos esperados
        writer.write(b"NACK"); await writer.drain()

    except Exception as e:
        print(f"⚠️ Error procesando mensaje del monitor: {e}")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

async def start_socket_server(producer: AIOKafkaProducer):
    server = await asyncio.start_server(
        lambda r, w: handle_monitor_connection(r, w, producer),
        host="0.0.0.0",
        port=ENGINE_PORT
    )
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets or [] )
    print(f"🕐 Escuchando conexiones del monitor en {addrs}...")
    async with server:
        await server.serve_forever()

# -------------------------------------------------------------
# ESCUCHA A CENTRAL (Kafka)
# -------------------------------------------------------------
async def listen_to_central(consumer, producer):
    global CHARGE_TASK
    print("👂 Esperando mensajes de CENTRAL...")
    try:
        async for msg in consumer:
            
            data = msg.value

            if msg.topic == "central.authorize":
                # 1) Solo si es para este CP
                if data.get("cp_id") != CP_ID:
                    continue
                # 2) No aceptes authorize si el CP no está ACTIVADO o está en STOP
                if STOP.is_set() or STATUS != "ACTIVADO":
                    print("⛔ Ignoro authorize: CP no disponible (PARADO/NO ACTIVO).")
                    continue
                # 3) Evitar duplicar sesiones si ya está suministrando (por si llegan dobles)
                if STATUS == "SUMINISTRANDO" or (CHARGE_TASK and not CHARGE_TASK.done()):
                    print("ℹ️ Ya estaba suministrando; ignoro authorize duplicado.")
                    continue

                print("🔔 Autorizado suministro por CENTRAL")
                CHARGE_TASK = asyncio.create_task(start_charging(producer))


            elif msg.topic == "central.command":
                action = data.get("action", "").upper()
                target = data.get("cp_id", "ALL")
                print(f"⚙️ Orden recibida desde CENTRAL: {action} → {target}")

                # aquí aceptamos global o dirigido a este CP
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

# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
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

    # Hilo de teclado (KO/OK) sin bloquear el loop
    threading.Thread(target=control_thread, daemon=True).start()

    # Tareas concurrentes
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
        try:
            await consumer.stop()
        except Exception:
            pass
        try:
            await producer.stop()
        except Exception:
            pass

# -------------------------------------------------------------
# EJECUCIÓN
# -------------------------------------------------------------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n🔴 Engine en puerto {ENGINE_PORT} detenido por el usuario.")
