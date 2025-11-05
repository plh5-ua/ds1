import asyncio, json, sys, random
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

# -------------------------------------------------------------
# ARGUMENTOS
# -------------------------------------------------------------
if len(sys.argv) < 2:
    print("Uso: python EV_CP_E.py <broker_ip:puerto>")
    sys.exit(1)

BROKER = sys.argv[1]
ENGINE_PORT = 6000  # puerto donde escucha al monitor

# -------------------------------------------------------------
# VARIABLES GLOBALES
# -------------------------------------------------------------
CP_ID = None
CP_LOCATION = "Desconocida"
STATUS = "DESCONECTADO"
PRICE = round(random.uniform(0.40, 0.70), 2)

# Señal de parada limpia
STOP = asyncio.Event()

# -------------------------------------------------------------
# ENVÍO DE DATOS A CENTRAL (Kafka)
# -------------------------------------------------------------
async def register_cp(producer):
    msg = {"cp_id": CP_ID, "location": CP_LOCATION, "kwh": PRICE, "status": STATUS}
    await producer.send_and_wait("cp.register", json.dumps(msg).encode())
    print(f"📡 Registrando CP {CP_ID} en CENTRAL ({CP_LOCATION}) - {PRICE} €/kWh")

async def send_status(producer, status):
    global STATUS
    STATUS = status
    payload = {"cp_id": CP_ID, "status": status}
    await producer.send_and_wait("cp.status", json.dumps(payload).encode())
    print(f"📡 Estado actualizado: {status}")

async def start_charging(producer):
    global STATUS
    STATUS = "SUMINISTRANDO"
    await send_status(producer, STATUS)
    kwh = 0.0
    print(f"⚡ Iniciando carga en {CP_ID} (Precio: {PRICE} €/kWh)")

    # 10 “segundos” de carga simulada
    for _ in range(10):
        if STOP.is_set():
            break
        kw = round(random.uniform(6.0, 7.5), 2)
        kwh += kw / 3600
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

    end_msg = {
        "cp_id": CP_ID,
        "session_id": 1,
        "kwh": round(kwh, 3),
        "amount_eur": round(kwh * PRICE, 2),
        "reason": "ENDED" if not STOP.is_set() else "ABORTED"
    }
    await producer.send_and_wait("cp.session_ended", json.dumps(end_msg).encode())
    await send_status(producer, "ACTIVADO")
    print("✅ Carga finalizada.")

# -------------------------------------------------------------
# SERVIDOR TCP ASÍNCRONO (Monitor ↔ Engine)
# -------------------------------------------------------------
async def handle_monitor_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, producer: AIOKafkaProducer):
    global CP_ID, CP_LOCATION
    try:
        data = await reader.read(4096)
        if not data:
            writer.close()
            await writer.wait_closed()
            return

        message = data.decode()

        # Respuesta a PING
        if message == "PING":
            writer.write(b"OK")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return

        payload = json.loads(message)

        # Registro inicial (ID y localización)
        if "cp_id" in payload and "location" in payload:
            CP_ID = payload["cp_id"]
            CP_LOCATION = payload["location"]
            print(f"✅ Recibido ID {CP_ID} y ubicación {CP_LOCATION}")
            await register_cp(producer)
            await send_status(producer, "ACTIVADO")

        # Comandos del monitor
        elif "action" in payload:
            action = payload["action"].upper()
            print(f"⚙️ Orden local recibida del monitor: {action}")
            if action == "PARAR":
                await send_status(producer, "PARADO")
            elif action == "ACTIVAR":
                await send_status(producer, "ACTIVADO")
            elif action == "SUMINISTRAR":
                await start_charging(producer)

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
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    print(f"🕐 Escuchando conexiones del monitor en {addrs}...")
    async with server:
        await server.serve_forever()

# -------------------------------------------------------------
# ESCUCHAR A CENTRAL (Kafka)
# -------------------------------------------------------------
async def listen_to_central(consumer: AIOKafkaConsumer, producer: AIOKafkaProducer):
    print("👂 Esperando mensajes de CENTRAL...")
    try:
        async for msg in consumer:
            if STOP.is_set():
                break
            data = msg.value
            # Ignorar mensajes de otros CPs hasta que tengamos ID
            if CP_ID is None or data.get("cp_id") != CP_ID:
                continue

            if msg.topic == "central.authorize":
                print("🔔 Autorizado suministro por CENTRAL")
                await start_charging(producer)
            elif msg.topic == "central.command":
                action = data.get("action", "").upper()
                print(f"⚙️ Orden recibida desde CENTRAL: {action}")
                if action == "PARAR":
                    await send_status(producer, "PARADO")
                elif action == "REANUDAR":
                    await send_status(producer, "ACTIVADO")
    except asyncio.CancelledError:
        # Salida limpia al cancelar la tarea
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
        group_id="engine-consumers",
        auto_offset_reset="latest"
    )

    await producer.start()
    await consumer.start()
    print(f"🔌 EV_CP_E conectado a Kafka {BROKER}")

    # Tareas en paralelo
    socket_task = asyncio.create_task(start_socket_server(producer))
    central_task = asyncio.create_task(listen_to_central(consumer, producer))

    try:
        # Esperar hasta Ctrl+C o parada externa
        await asyncio.gather(socket_task, central_task)
    except KeyboardInterrupt:
        print("\n🛑 Interrumpido por teclado. Cerrando…")
    finally:
        # Señal de parada y cancelación de tareas
        STOP.set()
        for t in (socket_task, central_task):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass

        # Cierre Kafka
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
        # Por si llegara hasta aquí
        print("\n🔴 Programa detenido por el usuario.")
