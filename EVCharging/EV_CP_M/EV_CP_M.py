import asyncio, json, sys, random
from aiokafka import AIOKafkaProducer

if len(sys.argv) < 4:
    print("Uso: python EV_CP_M.py <ip_engine:puerto> <ip_central:puerto> <id_cp>")
    sys.exit(1)

ENGINE_ADDR = sys.argv[1]
CENTRAL_ADDR = sys.argv[2]
CP_ID = sys.argv[3]
BROKER = "192.168.56.1:9092"  # no me convence esta solucion revisar

async def main():
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    await producer.start()
    print(f"🩺 EV_CP_M {CP_ID} monitorizando {ENGINE_ADDR} y conectado a CENTRAL {CENTRAL_ADDR}")
    try:
        while True:
            # enviar heartbeat
            heartbeat = {"cp_id": CP_ID, "health": "OK"}
            await producer.send_and_wait("cp.heartbeat", json.dumps(heartbeat).encode())
            print(f"❤️‍🔥 Heartbeat {CP_ID}")
            await asyncio.sleep(1)
    finally:
        await producer.stop()

asyncio.run(main())
