# EV_Driver.py (una pasada en modo fichero)
import sys, json, asyncio, uuid, os
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

USO = "Uso: python EV_Driver.py <broker_ip:puerto> <id_cliente> [fichero_cp_ids]"

# --------------------------- utilidades ---------------------------

def leer_fichero_cps(path: str):
    """Lee un fichero de texto con CPs (uno por línea). Devuelve lista de IDs."""
    if not os.path.exists(path):
        print(f"❌ Fichero no encontrado: {path}")
        return []
    ids = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            ids.append(t)
    if not ids:
        print("⚠️ El fichero no contenía CPs válidos.")
    else:
        print(f"📄 Cargados {len(ids)} CP(s) de {path}")
    return ids

async def solicitar_una_carga(producer, consumer, driver_id, cp, pausa_entre=4, timeout_sesion=180):
    """
    Lanza una solicitud para un CP y espera a que termine (FINISHED o DENIED/ERROR).
    Devuelve True si terminó; False si hubo error grave al consumir de Kafka.
    """
    req_id = str(uuid.uuid4())

    # 1) Solicitud a CENTRAL
    await producer.send_and_wait(
        "driver.request",
        json.dumps({
            "cp_id": cp,
            "driver_id": driver_id,
            "request_id": req_id
        }).encode()
    )
    print(f"📨 Solicitud enviada → CP={cp} | request_id={req_id}")

    # 2) Espera de esta sesión (updates + telemetrías)
    loop = asyncio.get_running_loop()
    fin = loop.time() + timeout_sesion

    while loop.time() < fin:
        try:
            msg = await asyncio.wait_for(consumer.getone(), timeout=1.0)
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            print(f"⚠️ Error recibiendo de Kafka: {e}")
            return False

        data = msg.value
        # Filtrado estricto por este driver y esta request
        if data.get("driver_id") != driver_id or data.get("request_id") != req_id:
            continue

        if msg.topic == "driver.update":
            status = (data.get("status") or "").upper()
            message = data.get("message", "")
            print(f"🔔 Update: {status} — {message}")

            if status in {"DENIED", "ERROR"}:
                # Rechazado o error → concluimos esta solicitud
                break

        if status == "FINISHED":
            summary = data.get("summary", {}) or {}
            kwh = summary.get("kwh")
            eur = summary.get("amount_eur")
            reason = summary.get("reason", "ENDED")
            loc = summary.get("location", "Desconocida")
            unit_price = summary.get("price_eur_kwh")
            cp_from_summary = summary.get("cp_id", cp)

            # Fallback de cálculo si amount_eur no vino y tenemos precio
            if eur is None and (kwh is not None) and (unit_price is not None):
                try:
                    eur = round(float(kwh) * float(unit_price), 2)
                except Exception:
                    pass

            print("\n===== TICKET DE RECARGA =====")
            print(f"CP:           {cp_from_summary}")
            print(f"Localización: {loc}")
            if unit_price is not None:
                print(f"Precio/kWh:   {unit_price} €")
            if kwh is not None:
                try:
                    print(f"Energía:      {float(kwh):.4f} kWh")
                except Exception:
                    print(f"Energía:      {kwh} kWh")
            if eur is not None:
                try:
                    print(f"Importe:      {float(eur):.2f} €")
                except Exception:
                    print(f"Importe:      {eur} €")
            print(f"Estado:       {reason}")
            print("=============================\n")

            # Mantén el break para cerrar el bucle de espera de esta solicitud
            break


        elif msg.topic == "driver.telemetry":
            # Progreso en tiempo real
            kw = data.get("kw")
            kwh_total = data.get("kwh_total")
            eur_total = data.get("eur_total")
            try:
                print(f"⚡ {cp} → {kw} kW | {kwh_total:.4f} kWh | {eur_total:.2f} €")
            except Exception:
                print(f"⚡ {cp} → {kw} kW | {kwh_total} kWh | {eur_total} €")

    else:
        print("⏱️  Timeout esperando conclusión; continúo con el siguiente tras 4 s…")

    # 3) Pausa entre sesiones (requisito)
    await asyncio.sleep(pausa_entre)
    return True

# --------------------------- bucles principales ---------------------------

async def run_modo_fichero_una_pasada(producer, consumer, driver_id, cp_ids):
    """Recorre la lista de CPs una sola vez (sin bucle infinito)."""
    print(f"🗂️  Modo fichero: {len(cp_ids)} CP(s) → una pasada.")
    for cp in cp_ids:
        ok = await solicitar_una_carga(producer, consumer, driver_id, cp)
        if not ok:
            print("⚠️ Se interrumpió la sesión por error; continúo con el siguiente CP…")
            await asyncio.sleep(2)

async def run_modo_interactivo(producer, consumer, driver_id):
    """
    Modo interactivo:
      - Escribe un CP y Enter para una sola carga.
      - ':load RUTA' para cargar un fichero y lanzar UNA PASADA.
      - 'q' o vacío para salir.
    """
    print("💬 Modo interactivo. Escribe un CP y pulsa Enter.")
    print("   Comandos: ':load RUTA' (una pasada desde fichero) | 'q' para salir.")
    while True:
        line = input("CP a solicitar (o ':load RUTA'): ").strip()
        if not line or line.lower() == "q":
            print("👋 Saliendo del modo interactivo.")
            break

        if line.lower().startswith(":load "):
            ruta = line.split(" ", 1)[1].strip()
            ids = leer_fichero_cps(ruta)
            if ids:
                try:
                    await run_modo_fichero_una_pasada(producer, consumer, driver_id, ids)
                except KeyboardInterrupt:
                    print("\n🛑 Pasada interrumpida. Vuelves al modo interactivo.")
            continue

        # Una solicitud directa
        await solicitar_una_carga(producer, consumer, driver_id, line)

async def run(broker, driver_id, cp_ids=None):
    """Si cp_ids != None: hace UNA PASADA por el fichero y luego entra en modo interactivo.
       Si cp_ids == None: entra directamente en modo interactivo."""
    producer = AIOKafkaProducer(bootstrap_servers=broker)
    consumer = AIOKafkaConsumer(
        "driver.update",
        "driver.telemetry",
        bootstrap_servers=broker,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
        group_id=f"driver-{driver_id}"
    )

    await producer.start()
    await consumer.start()
    print(f"🚗 EV_Driver listo → broker={broker} | driver={driver_id}")

    try:
        if cp_ids:
            # 1) Una pasada por el fichero
            await run_modo_fichero_una_pasada(producer, consumer, driver_id, cp_ids)
            print("✔️ Pasada de fichero completada.")

        # 2) Siempre entramos en interactivo después
        print("🎛️ Entrando en modo interactivo (Ctrl+C o 'q' para salir).")
        await run_modo_interactivo(producer, consumer, driver_id)

    finally:
        await consumer.stop()
        await producer.stop()

# --------------------------- entrada ---------------------------

def main():
    if len(sys.argv) < 3:
        print(USO)
        sys.exit(1)

    broker = sys.argv[1]
    driver_id = sys.argv[2]


    cp_ids = None
    if len(sys.argv) >= 4:
        cp_ids = leer_fichero_cps(sys.argv[3])
        if not cp_ids:
            cp_ids = None
    else:
        ruta = input("Ruta del fichero con CPs (Enter para modo interactivo): ").strip()
        if ruta:
            cp_ids = leer_fichero_cps(ruta)
            if not cp_ids:
                cp_ids = None

    try:
        asyncio.run(run(broker, driver_id, cp_ids))
    except KeyboardInterrupt:
        print("\n Cancelado por el usuario.")

if __name__ == "__main__":
    main()
