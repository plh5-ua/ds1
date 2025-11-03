import asyncio, json, sys, socket, random
from aiokafka import AIOKafkaProducer

if len(sys.argv) < 5:
    print("Uso: python EV_CP_M.py <ip_engine:puerto> <ip_central:puerto> <ip_broker:puerto> <id_cp>")
    sys.exit(1)

ENGINE_ADDR = sys.argv[1]
CENTRAL_ADDR = sys.argv[2]
BROKER = sys.argv[3]
CP_ID = sys.argv[4]

ENGINE_IP, ENGINE_PORT = ENGINE_ADDR.split(":")
ENGINE_PORT = int(ENGINE_PORT)

async def send_heartbeat(producer, health_flag):
    heartbeat = {"cp_id": CP_ID, "health": health_flag}
    await producer.send_and_wait("cp.heartbeat", json.dumps(heartbeat).encode())
    print(f"❤️‍🔥 Heartbeat {CP_ID} ({health_flag})")

def send_id_to_engine():
    """Envía el ID del CP al Engine por socket TCP."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ENGINE_IP, ENGINE_PORT))
            s.sendall(json.dumps({"cp_id": CP_ID}).encode())
            print(f"📨 ID {CP_ID} enviado al Engine ({ENGINE_IP}:{ENGINE_PORT})")
    except ConnectionRefusedError:
        print("⚠️ No se pudo conectar con el Engine. ¿Está en ejecución?")

async def main():
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    await producer.start()
    print(f"🩺 EV_CP_M {CP_ID} monitorizando {ENGINE_ADDR}, CENTRAL {CENTRAL_ADDR}")
    print(f"Broker Kafka: {BROKER}\n")

    health = "OK"

    try:
        while True:
            print("\n=== MENÚ MONITOR ===")
            print("1️⃣  Enviar ID al Engine")
            print("2️⃣  Enviar Heartbeat a CENTRAL")
            print("3️⃣  Simular avería (KO)")
            print("4️⃣  Recuperar (OK)")
            print("5️⃣  Salir")
            op = input("Selecciona opción: ").strip()

            if op == "1":
                send_id_to_engine()
            elif op == "2":
                await send_heartbeat(producer, health)
            elif op == "3":
                health = "KO"
                await send_heartbeat(producer, health)
            elif op == "4":
                health = "OK"
                await send_heartbeat(producer, health)
            elif op == "5":
                print("👋 Cerrando monitor.")
                break
            else:
                print("❌ Opción no válida.")
    finally:
        await producer.stop()

asyncio.run(main())
