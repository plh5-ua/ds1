import asyncio, json, sys, random
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

if len(sys.argv) < 2:
    print("Uso: python EV_CP_E.py <broker_ip:puerto>")
    sys.exit(1)

BROKER = sys.argv[1]
CP_ID = input("Introduce el ID del punto de recarga: ").strip()
CP_LOCATION= input("Introduce la localización del punto de recarga: ").strip()


STATUS = "ACTIVADO"


async def send_register(producer):
    payload = {
        "id": CP_ID,
        "location": CP_LOCATION,
        "kwh": round(random.uniform(0.40, 0.70), 2),
        "status": "PARADO"
    }
    await producer.send_and_wait("cp.register", json.dumps(payload).encode())
    print(f"Charge point correctamente registrado: ID:{CP_ID} en la localizacion: {CP_LOCATION}")


async def main():
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    consumer = AIOKafkaConsumer(
        "central.authorize", "central.command",
        bootstrap_servers=BROKER,
        value_deserializer=lambda b: json.loads(b.decode())
    )
    await producer.start()
    await send_register(producer)
    await consumer.start()
    print(f"EV_CP_E {CP_ID} conectado a {BROKER}")
    try:
        async for msg in consumer:
            data = msg.value
            # Filtra mensajes dirigidos a este CP
            if data.get("cp_id") != CP_ID:
                continue
            if msg.topic == "central.authorize":
                print("Autorizado suministro por CENTRAL")
                await start_charging(producer, data)
            elif msg.topic == "central.command":
                print(f"Orden recibida: {data['action']}")
                if data["action"].upper() == "PARAR":
                    await send_status(producer, "PARADO")
                elif data["action"].upper() == "REANUDAR":
                    await send_status(producer, "ACTIVADO")
    finally:
        await consumer.stop()
        await producer.stop()




async def send_status(producer, status):
    global STATUS
    STATUS = status
    payload = {"cp_id": CP_ID, "status": status}
    await producer.send_and_wait("cp.status", json.dumps(payload).encode())
    print(f"📡 Estado actualizado: {status}")

async def start_charging(producer, data):
    global STATUS
    STATUS = "SUMINISTRANDO"
    await send_status(producer, STATUS)

    kwh = 0
    price = 0.30
    for _ in range(10):  # Simular 10 segundos de carga
        kw = round(random.uniform(6.0, 7.5), 2)
        kwh += kw / 3600  # aprox cada segundo
        amount = round(kwh * price, 3)
        telem = {
            "cp_id": CP_ID,
            "session_id": random.randint(1000, 9999),
            "kw": kw,
            "kwh_total": round(kwh, 3),
            "eur_total": amount
        }
        await producer.send_and_wait("cp.telemetry", json.dumps(telem).encode())
        print(f"⚡ {CP_ID}: {kw} kW  {amount:.2f} €")
        await asyncio.sleep(1)

    # Fin de carga
    end_msg = {
        "cp_id": CP_ID,
        "session_id": random.randint(1000, 9999),
        "kwh": round(kwh, 3),
        "amount_eur": round(kwh * price, 2),
        "reason": "ENDED"
    }
    await producer.send_and_wait("cp.session_ended", json.dumps(end_msg).encode())
    await send_status(producer, "ACTIVADO")

asyncio.run(main())
