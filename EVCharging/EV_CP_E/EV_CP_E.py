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
STATUS = "PARADO"
PRICE = round(random.uniform(0.40, 0.70), 2)

# -------------------------------------------------------------
# SOCKET: recibir ID desde el monitor
# -------------------------------------------------------------
def wait_for_id_location_from_monitor():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", ENGINE_PORT))
    server.listen(1)
    print(f"🕐 Esperando ID desde el monitor (puerto {ENGINE_PORT})...")
    conn, addr = server.accept()
    data = conn.recv(1024)
    cp_info = json.loads(data.decode())
    global CP_ID
    global CP_LOCATION
    CP_ID = cp_info.get("id")
    CP_LOCATION = cp_info.get("location")
    print(f"✅ ID recibido del monitor: {CP_ID} y location: {CP_LOCATION}")
    conn.close()
    server.close()

# -------------------------------------------------------------
# ENVÍO DE DATOS A CENTRAL (Kafka)
# -------------------------------------------------------------
async def register_cp(producer):
    msg = {
        "id": CP_ID,
        "location": CP_LOCATION,
        "kwh": PRICE,
        "status": STATUS
    }
    await producer.send_and_wait("cp.register", json.dumps(msg).encode())
    print(f"📡 Registrando CP {CP_ID} en CENTRAL ({CP_LOCATION}) - {PRICE} €/kWh")

async def send_status(producer, status):
    global STATUS
    STATUS = status
    payload = {"id": CP_ID, "status": status}
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
            "id": CP_ID,
            "session_id": random.randint(1000, 9999),
            "kw": kw,
            "kwh_total": round(kwh, 3),
            "eur_total": amount
        }
        await producer.send_and_wait("cp.telemetry", json.dumps(telem).encode())
        print(f"🔋 {CP_ID}: {kw} kW — {amount:.3f} €")
        await asyncio.sleep(1)

    end_msg = {
        "id": CP_ID,
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
        if data.get("id") != CP_ID:
            continue
        if msg.topic == "central.authorize":
            print("🔔 Autorizado suministro por CENTRAL")
            await start_charging(producer)
        elif msg.topic == "central.command":
            action = data["action"].upper()
            print(f"⚙️ Orden recibida: {action}")
            if action == "PARAR":
                await send_status(producer, "PARADO")
            elif action == "REANUDAR":
                await send_status(producer, "ACTIVADO")

# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
async def main():
    # 1️⃣ Esperar ID desde el monitor (bloqueante)
    wait_for_id_location_from_monitor()

    # 2️⃣ Iniciar Kafka
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    consumer = AIOKafkaConsumer(
        "central.authorize", "central.command",
        bootstrap_servers=BROKER,
        value_deserializer=lambda b: json.loads(b.decode())
    )

    await producer.start()
    await consumer.start()
    print(f"🔌 EV_CP_E {CP_ID} conectado a Kafka {BROKER}")
    print(f"📍 Precio: {PRICE} €/kWh")

    # 3️⃣ Registrar CP en CENTRAL
    await register_cp(producer)
    await send_status(producer, "ACTIVADO")

    # 4️⃣ Escuchar CENTRAL
    await listen_to_central(consumer, producer)

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
