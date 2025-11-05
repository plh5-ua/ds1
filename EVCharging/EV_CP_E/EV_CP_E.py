import asyncio, json, sys, random, socket, threading
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
HEALTH = "OK"

# -------------------------------------------------------------
# CONTROL DESDE TECLADO (para simular KO/OK)
# -------------------------------------------------------------
def control_thread():
    global HEALTH
    while True:
        cmd = input("").strip().upper()
        if cmd == "KO":
            HEALTH = "KO"
            print("⚠️ Engine simulado en estado KO.")
        elif cmd == "OK":
            HEALTH = "OK"
            print("✅ Engine vuelve a estado OK.")

# -------------------------------------------------------------
# SOCKET SERVER (comunicación con Monitor)
# -------------------------------------------------------------
async def socket_server():
    """Recibe registro e instrucciones desde el Monitor."""
    global CP_ID, CP_LOCATION
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("0.0.0.0", ENGINE_PORT))
    server.listen(5)
    print(f"🕐 Escuchando al monitor en puerto {ENGINE_PORT}...")

    while True:
        conn, addr = server.accept()
        data = conn.recv(1024)
        if not data:
            conn.close()
            continue

        msg = data.decode().strip()
        # Heartbeat del monitor
        if msg == "HEARTBEAT":
            conn.sendall(HEALTH.encode())
            conn.close()
            continue

        # Mensaje de registro inicial
        try:
            payload = json.loads(msg)
            if "cp_id" in payload and "location" in payload:
                CP_ID = payload["cp_id"]
                CP_LOCATION = payload["location"]
                conn.sendall(b"ACK")
                print(f"✅ Recibido ID {CP_ID} ({CP_LOCATION}) del monitor")
        except Exception as e:
            print(f"⚠️ Error procesando mensaje: {e}")
        conn.close()

# -------------------------------------------------------------
# KAFKA (comunicación con CENTRAL)
# -------------------------------------------------------------
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

async def listen_to_central(producer):
    consumer = AIOKafkaConsumer(
        "central.authorize", "central.command",
        bootstrap_servers=BROKER,
        value_deserializer=lambda b: json.loads(b.decode())
    )
    await consumer.start()
    print(f"📡 Escuchando mensajes de CENTRAL para {ENGINE_PORT}...")

    try:
        async for msg in consumer:
            data = msg.value
            if data.get("cp_id") != CP_ID:
                continue
            if msg.topic == "central.authorize":
                print("🔔 Autorizado suministro por CENTRAL")
                await start_charging(producer)
            elif msg.topic == "central.command":
                action = data["action"].upper()
                print(f"⚙️ Orden desde CENTRAL: {action}")
                if action == "PARAR":
                    await send_status(producer, "PARADO")
                elif action == "REANUDAR":
                    await send_status(producer, "ACTIVADO")
    finally:
        await consumer.stop()

# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
async def main():
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    await producer.start()

    threading.Thread(target=control_thread, daemon=True).start()

    print(f"🔌 EV_CP_E (Engine) ejecutándose en puerto {ENGINE_PORT}")
    print(f"⌨️ Escribe 'KO' o 'OK' para simular fallos en este Engine.\n")

    await asyncio.gather(
        socket_server(),
        listen_to_central(producer)
    )

    await producer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n🔴 Engine en puerto {ENGINE_PORT} detenido por el usuario.")
