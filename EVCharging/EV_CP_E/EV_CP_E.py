import asyncio, json, sys, random, socket
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

# -------------------------------------------------------------
# SOCKET: Servidor que escucha permanentemente al monitor
# -------------------------------------------------------------
async def socket_server(producer):
    """Servidor TCP para recibir ID inicial, comandos y responder PING."""
    global CP_ID, CP_LOCATION
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", ENGINE_PORT))
    server.listen(5)
    print(f"🕐 Escuchando conexiones del monitor en puerto {ENGINE_PORT}...")

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024)
        if not data:
            conn.close()
            continue

        try:
            message = data.decode()
            # Si el monitor manda "PING", responder OK
            if message == "PING":
                conn.sendall(b"OK")
                conn.close()
                continue

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
            conn.close()

# -------------------------------------------------------------
# ENVÍO DE DATOS A CENTRAL (Kafka)
# -------------------------------------------------------------
async def register_cp(producer):
    msg = {
        "cp_id": CP_ID,
        "location": CP_LOCATION,
        "kwh": PRICE,
        "status": STATUS
    }
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
    kwh = 0
    print(f"⚡ Iniciando carga en {CP_ID} (Precio: {PRICE} €/kWh)")

    for _ in range(10):
        kw = round(random.uniform(6.0, 7.5), 2)
        kwh += kw / 3600
        amount = round(kwh * PRICE, 3)
        telem = {
            "cp_id": CP_ID,
            "session_id": random.randint(1000, 9999),
            "kw": kw,
            "kwh_total": round(kwh, 3),
            "eur_total": amount
        }
        await producer.send_and_wait("cp.telemetry", json.dumps(telem).encode())
        print(f"🔋 {CP_ID}: {kw} kW — {amount:.3f} €")
        await asyncio.sleep(1)

    end_msg = {
        "cp_id": CP_ID,
        "session_id": random.randint(1000, 9999),
        "kwh": round(kwh, 3),
        "amount_eur": round(kwh * PRICE, 2),
        "reason": "ENDED"
    }
    await producer.send_and_wait("cp.session_ended", json.dumps(end_msg).encode())
    await send_status(producer, "ACTIVADO")
    print("✅ Carga finalizada.")

# -------------------------------------------------------------
# ESCUCHAR A CENTRAL (Kafka)
# -------------------------------------------------------------
async def listen_to_central(consumer, producer):
    async for msg in consumer:
        data = msg.value
        if data.get("cp_id") != CP_ID:
            continue
        if msg.topic == "central.authorize":
            print("🔔 Autorizado suministro por CENTRAL")
            await start_charging(producer)
        elif msg.topic == "central.command":
            action = data["action"].upper()
            print(f"⚙️ Orden recibida desde CENTRAL: {action}")
            if action == "PARAR":
                await send_status(producer, "PARADO")
            elif action == "REANUDAR":
                await send_status(producer, "ACTIVADO")

# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
async def main():
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    consumer = AIOKafkaConsumer(
        "central.authorize", "central.command",
        bootstrap_servers=BROKER,
        value_deserializer=lambda b: json.loads(b.decode())
    )

    await producer.start()
    await consumer.start()
    print(f"🔌 EV_CP_E conectado a Kafka {BROKER}")

    # Ejecutar socket server (local con el monitor) y escucha de CENTRAL en paralelo
    await asyncio.gather(
        socket_server(producer),
        listen_to_central(consumer, producer)
    )

    await consumer.stop()
    await producer.stop()

# -------------------------------------------------------------
# EJECUCIÓN
# -------------------------------------------------------------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🔴 Programa detenido por el usuario.")
