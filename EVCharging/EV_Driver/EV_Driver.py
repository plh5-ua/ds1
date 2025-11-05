import sys, json, asyncio, uuid
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

USO = "Uso: python EV_Driver.py <broker_ip:puerto> <id_cliente> [fichero_cp_ids]"

async def run(broker, driver_id, cp_ids):
    producer = AIOKafkaProducer(bootstrap_servers=broker)
    consumer = AIOKafkaConsumer(
        "driver.update",
        "driver.telemetry",
        bootstrap_servers=broker,
        value_deserializer=lambda b: json.loads(b.decode("utf-8"))
    )

    await producer.start()
    await consumer.start()
    print(f"🚗 EV_Driver listo → broker={broker} | driver={driver_id}")

    try:
        for cp in cp_ids:
            req_id = str(uuid.uuid4())

            # 1) Enviar solicitud a CENTRAL
            await producer.send_and_wait(
                "driver.request",
                json.dumps({
                    "cp_id": cp,
                    "driver_id": driver_id,
                    "request_id": req_id
                }).encode()
            )
            print(f"📨 Solicitud enviada → CP={cp} | request_id={req_id}")

            # 2) Esperar respuestas de esta solicitud (updates + telemetrías)
            while True:
                msg = await consumer.getone()
                data = msg.value

                # Filtrar estrictamente por este driver y este request
                if data.get("driver_id") != driver_id or data.get("request_id") != req_id:
                    continue

                if msg.topic == "driver.update":
                    status = data.get("status")
                    message = data.get("message", "")
                    print(f"🔔 Update: {status} — {message}")

                    if status in {"DENIED", "ERROR"}:
                        # Petición rechazada o error → pasamos al siguiente CP
                        break

                    if status == "FINISHED":
                        # Ticket final (resumen)
                        summary = data.get("summary", {})
                        kwh = summary.get("kwh")
                        eur = summary.get("amount_eur")
                        reason = summary.get("reason", "ENDED")
                        print(f"✅ Carga finalizada en {cp} → {kwh} kWh | {eur} € (reason={reason})")
                        break

                elif msg.topic == "driver.telemetry":
                    # Progreso en tiempo real
                    kw = data.get("kw")
                    kwh_total = data.get("kwh_total")
                    eur_total = data.get("eur_total")
                    print(f"⚡ {cp} → {kw} kW | {kwh_total} kWh | {eur_total:.2f} €")

            # 3) Pausa de 4 s entre solicitudes (requisito de la práctica)
            await asyncio.sleep(4)

    finally:
        await consumer.stop()
        await producer.stop()

def main():
    if len(sys.argv) < 3:
        print(USO); sys.exit(1)

    broker = sys.argv[1]
    driver_id = sys.argv[2]

    if len(sys.argv) >= 4:
        with open(sys.argv[3], "r", encoding="utf-8") as f:
            cp_ids = [line.strip() for line in f if line.strip()]
    else:
        cp = input("CP a solicitar: ").strip()
        cp_ids = [cp]

    asyncio.run(run(broker, driver_id, cp_ids))

if __name__ == "__main__":
    main()
