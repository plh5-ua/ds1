# EV_CP_E.py — Engine con menú 1–4 (Windows/Linux)
import asyncio, json, sys, random, threading
from collections import deque
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
# ESTADO GLOBAL
# -------------------------------------------------------------
CP_ID = None
CP_LOCATION = "Desconocida"
STATUS = "DESCONECTADO"
PRICE = round(random.uniform(0.40, 0.70), 2)
HEALTH = "OK"               # expuesto vía HEARTBEAT local
STOP = asyncio.Event()      # señal para detener carga
CHARGE_TASK = None          # asyncio.Task | None

# Entrada de usuario (único lector)
INPUT_Q: asyncio.Queue[str] = asyncio.Queue()
PROMPT_FUT: asyncio.Future | None = None

# Buffer de drivers y autorización
AVAILABLE_DRIVERS = deque(maxlen=50)   # [{'driver_id','request_id','cp_id','ts'}]
PENDING_AUTH = None                    # {'driver_id','request_id','cp_id'}

# -------------------------------------------------------------
# UTILIDADES DE TECLADO (no bloqueantes)
# -------------------------------------------------------------
def stdin_reader(loop: asyncio.AbstractEventLoop):
    """Único lector de stdin. Si hay un ask() esperando, le entrega la línea;
       si no, la manda a la cola de comandos del menú."""
    global PROMPT_FUT
    while True:
        try:
            line = input()
        except EOFError:
            break
        line = (line or "").strip()
        fut = PROMPT_FUT
        if fut is not None and not fut.done():
            loop.call_soon_threadsafe(fut.set_result, line)
        else:
            asyncio.run_coroutine_threadsafe(INPUT_Q.put(line), loop)

async def ask(prompt: str) -> str:
    """Muestra un prompt y espera una respuesta de teclado sin bloquear el loop."""
    global PROMPT_FUT
    print(prompt, end="", flush=True)
    PROMPT_FUT = asyncio.get_running_loop().create_future()
    try:
        resp = await PROMPT_FUT
        print("")  # salto de línea después de la respuesta
        return resp
    finally:
        PROMPT_FUT = None

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

# -------------------------------------------------------------
# CARGA (con confirmación de fin)
# -------------------------------------------------------------
async def start_charging(producer: AIOKafkaProducer):
    """Inicia carga inmediatamente (la aceptación la hace el menú opción 4).
       Al terminar, pide confirmación por consola para cerrar la sesión."""
    global STATUS, CHARGE_TASK

    STATUS = "SUMINISTRANDO"
    await send_status(producer, STATUS)
    print(f"⚡ Iniciando carga en {CP_ID} (Precio: {PRICE} €/kWh)")

    kwh = 0.0
    for _ in range(50):
        if STOP.is_set():
            break
        kw = round(random.uniform(6.0, 7.5), 2)
        # 5 minutos simulados por segundo:
        kwh += kw / 3600 * 60 * 5
        amount = round(kwh * PRICE, 3)

        telem = {
            "cp_id": CP_ID,
            "session_id": 1,           # demo
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

    # Confirmación de fin
    while True:
        resp = (await ask(f"¿Confirmar fin de carga en CP {CP_ID}? (s/n): ")).lower()
        if resp == "s":
            break
        print("⏸ Fin NO confirmado. Reintentando en 5s…")
        await asyncio.sleep(5)

    end_msg = {
        "cp_id": CP_ID,
        "session_id": 1,
        "kwh": round(kwh, 3),
        "amount_eur": round(kwh * PRICE, 2),
        "reason": "ENDED" if not STOP.is_set() else "ABORTED"
    }
    await producer.send_and_wait("cp.session_ended", json.dumps(end_msg).encode())

    await send_status(producer, "PARADO" if STOP.is_set() else "ACTIVADO")
    print("✅ Carga finalizada.")
    print_menu()
    CHARGE_TASK = None

# -------------------------------------------------------------
# MENÚ
# -------------------------------------------------------------
def print_menu():
    print("\n===== MENÚ ENGINE =====")
    print("1) Activarse (health=OK / status=ACTIVADO)")
    print("2) Desactivar (KO)  (health=KO / status=PARADO)")
    print("3) Suministrar a un driver (mostrar disponibles)")
    print("4) Aceptar suministro (si hay autorización pendiente)")
    print("=======================\n")

async def menu_loop(producer: AIOKafkaProducer):
    global HEALTH, PENDING_AUTH, CHARGE_TASK, STATUS
    print_menu()
    while True:
        cmd = (await INPUT_Q.get()).strip()
        if cmd == "1":
            HEALTH = "OK"
            await send_status(producer, "ACTIVADO")
            print("✅ CP ACTIVADO (salud=OK).")
        elif cmd == "2":
            HEALTH = "KO"
            await send_status(producer, "PARADO")
            print("⛔ CP en KO (status PARADO).")
        elif cmd == "3":
            visibles = [d for d in list(AVAILABLE_DRIVERS) if d.get("cp_id") == CP_ID]
            if not visibles:
                print("ℹ️ No hay drivers disponibles para este CP ahora mismo.")
            else:
                print("👥 Drivers disponibles (últimos):")
                for d in visibles:
                    print(f"   - driver_id={d['driver_id']}  req={d['request_id']}")
        elif cmd == "4":
            if not PENDING_AUTH or PENDING_AUTH.get("cp_id") != CP_ID:
                print("⚠️ No hay autorización pendiente para este CP.")
            elif STATUS == "SUMINISTRANDO" or (CHARGE_TASK and not CHARGE_TASK.done()):
                print("ℹ️ Ya hay una carga en marcha.")
            else:
                print(f"✅ Aceptando suministro para driver {PENDING_AUTH['driver_id']} ...")
                CHARGE_TASK = asyncio.create_task(start_charging(producer))
                PENDING_AUTH = None
        else:
            print("❓ Opción no válida.")
        print_menu()

# -------------------------------------------------------------
# SOCKET (Monitor ↔ Engine)
# -------------------------------------------------------------
async def handle_monitor_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, producer: AIOKafkaProducer):
    global CP_ID, CP_LOCATION, HEALTH
    try:
        data = await reader.read(4096)
        if not data:
            return
        message = data.decode().strip()

        if message == "PING":
            writer.write(b"OK"); await writer.drain(); return
        if message == "HEARTBEAT":
            writer.write(HEALTH.encode()); await writer.drain(); return

        try:
            payload = json.loads(message)
        except Exception:
            writer.write(b"NACK"); await writer.drain(); return

        # Registro inicial
        if "cp_id" in payload and "location" in payload:
            CP_ID = payload["cp_id"]
            CP_LOCATION = payload["location"]
            print(f"✅ Recibido ID {CP_ID} y ubicación {CP_LOCATION}")
            writer.write(b"ACK"); await writer.drain()
            await send_status(producer, "ACTIVADO")
            return

        # Acciones locales
        if "action" in payload:
            action = (payload["action"] or "").upper()
            print(f"⚙️ Orden local del monitor: {action}")
            if action == "PARAR":
                await send_status(producer, "PARADO")
                writer.write(b"OK"); await writer.drain(); return
            elif action == "ACTIVAR":
                await send_status(producer, "ACTIVADO")
                writer.write(b"OK"); await writer.drain(); return
            elif action == "SUMINISTRAR":
                # Ojo: aquí saltas la autorización CENTRAL → solo demo local
                if not (CHARGE_TASK and not CHARGE_TASK.done()):
                    CHARGE_TASK = asyncio.create_task(start_charging(producer))
                writer.write(b"OK"); await writer.drain(); return
            elif action == "KO":
                HEALTH = "KO"; writer.write(b"OK"); await writer.drain(); return
            elif action == "OK":
                HEALTH = "OK"; writer.write(b"OK"); await writer.drain(); return
            else:
                writer.write(b"NACK"); await writer.drain(); return

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
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    print(f"🕐 Escuchando conexiones del monitor en {addrs}...")
    async with server:
        await server.serve_forever()

# -------------------------------------------------------------
# ESCUCHA A CENTRAL (Kafka)
# -------------------------------------------------------------
async def listen_to_central(consumer: AIOKafkaConsumer, producer: AIOKafkaProducer):
    global CHARGE_TASK, PENDING_AUTH
    print("👂 Esperando mensajes de CENTRAL...")
    try:
        async for msg in consumer:
            data = msg.value

            # Drivers que piden este CP (para mostrar en menú)
            if msg.topic == "driver.request":
                if data.get("cp_id") == CP_ID:
                    AVAILABLE_DRIVERS.append({
                        "driver_id": data.get("driver_id"),
                        "request_id": data.get("request_id"),
                        "cp_id": data.get("cp_id"),
                        "ts": asyncio.get_running_loop().time(),
                    })
                continue

            # CENTRAL autoriza este CP para un driver concreto
            if msg.topic == "central.authorize":
                if data.get("cp_id") != CP_ID:
                    continue
                PENDING_AUTH = {
                    "driver_id": data.get("driver_id"),
                    "request_id": data.get("request_id"),
                    "cp_id": data.get("cp_id"),
                }
                print("🟢 Autorización recibida para este CP. Usa opción 4 para aceptar y comenzar.")
                continue

            # STOP / RESUME
            if msg.topic == "central.command":
                action = (data.get("action") or "").upper()
                target = data.get("cp_id", "ALL")
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
        # Nota: añadimos 'driver.request' para poder mostrar drivers disponibles
        "driver.request", "central.authorize", "central.command",
        bootstrap_servers=BROKER,
        value_deserializer=lambda b: json.loads(b.decode()),
        group_id=f"engine-{ENGINE_PORT}",
        auto_offset_reset="latest"
    )

    await producer.start()
    await consumer.start()
    print(f"🔌 EV_CP_E conectado a Kafka {BROKER}")
    print(f"🧩 Puerto del Engine para monitor: {ENGINE_PORT}")

    # Lanzar lector de teclado y tareas concurrentes
    loop = asyncio.get_running_loop()
    threading.Thread(target=stdin_reader, args=(loop,), daemon=True).start()

    socket_task = asyncio.create_task(start_socket_server(producer))
    central_task = asyncio.create_task(listen_to_central(consumer, producer))
    menu_task   = asyncio.create_task(menu_loop(producer))

    try:
        await asyncio.gather(socket_task, central_task, menu_task)
    except KeyboardInterrupt:
        print("\n🛑 Interrumpido por teclado. Cerrando…")
    finally:
        STOP.set()
        for t in (socket_task, central_task, menu_task):
            t.cancel()
            try:
                await t
            except asyncio.CancelledError:
                pass
        await consumer.stop()
        await producer.stop()

# -------------------------------------------------------------
# EJECUCIÓN
# -------------------------------------------------------------
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n🔴 Engine en puerto {ENGINE_PORT} detenido por el usuario.")
