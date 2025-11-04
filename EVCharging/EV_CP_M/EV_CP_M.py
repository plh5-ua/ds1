import asyncio, json, sys, socket, random
from aiokafka import AIOKafkaProducer

if len(sys.argv) < 6:
    print("Uso: python EV_CP_M.py <ip_engine:puerto> <ip_central:puerto> <ip_broker:puerto> <id_cp> <location>")
    sys.exit(1)

ENGINE_ADDR = sys.argv[1]
CENTRAL_ADDR = sys.argv[2]
BROKER = sys.argv[3]
CP_ID = sys.argv[4]
LOCATION = sys.argv[5].replace("_", " ")

ENGINE_IP, ENGINE_PORT = ENGINE_ADDR.split(":")
ENGINE_PORT = int(ENGINE_PORT)


# ---------------------------------------------------------------------------
# FUNCIONES AUXILIARES
# ---------------------------------------------------------------------------

def send_id_to_engine():
    """Envía el ID y localización del CP al Engine por socket TCP."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ENGINE_IP, ENGINE_PORT))
            s.sendall(json.dumps({"cp_id": CP_ID, "location": LOCATION}).encode())
            print(f"📨 ID {CP_ID} ({LOCATION}) enviado al Engine ({ENGINE_IP}:{ENGINE_PORT})")
    except ConnectionRefusedError:
        print("⚠️ No se pudo conectar con el Engine. ¿Está en ejecución?")

def send_command_to_engine(command):
    """Envía comandos de control al Engine por socket TCP (por ejemplo, PARAR, ACTIVAR)."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((ENGINE_IP, ENGINE_PORT))
            s.sendall(json.dumps({"action": command}).encode())
            print(f"⚙️ Orden '{command}' enviada al Engine")
    except ConnectionRefusedError:
        print("⚠️ Engine no responde a la orden.")

def check_engine_connection(timeout=1):
    """Comprueba si el Engine responde (simula PING)."""
    try:
        with socket.create_connection((ENGINE_IP, ENGINE_PORT), timeout=timeout) as s:
            s.sendall(b"PING")
            return True
    except (ConnectionRefusedError, socket.timeout, OSError):
        return False

async def send_heartbeat(producer, health_flag):
    """Envía a CENTRAL el estado de salud (OK o KO) cada segundo."""
    heartbeat = {"cp_id": CP_ID, "health": health_flag}
    await producer.send_and_wait("cp.heartbeat", json.dumps(heartbeat).encode())
    print(f"❤️‍🔥 Heartbeat {CP_ID} ({health_flag})")


# ---------------------------------------------------------------------------
# LOOP AUTOMÁTICO DE HEARTBEATS
# ---------------------------------------------------------------------------

async def heartbeat_loop(producer):
    """Envía heartbeats automáticos cada segundo."""
    while True:
        alive = check_engine_connection()
        health = "OK" if alive else "KO"
        await send_heartbeat(producer, health)
        await asyncio.sleep(1)


# ---------------------------------------------------------------------------
# MENÚ DEL MONITOR
# ---------------------------------------------------------------------------

async def menu_monitor(producer):
    while True:
        print("\n=== MENÚ MONITOR ===")
        print("1️⃣  Registrarse en CENTRAL (enviar ID al Engine)")
        print("2️⃣  Activar punto de recarga")
        print("3️⃣  Parar punto de recarga")
        print("4️⃣  Iniciar carga manual (simulada)")
        print("5️⃣  Salir")
        op = input("Selecciona opción: ").strip()

        if op == "1":
            send_id_to_engine()
        elif op == "2":
            send_command_to_engine("ACTIVAR")
        elif op == "3":
            send_command_to_engine("PARAR")
        elif op == "4":
            send_command_to_engine("SUMINISTRAR")
        elif op == "5":
            print("👋 Cerrando monitor.")
            break
        else:
            print("❌ Opción no válida.")


# ---------------------------------------------------------------------------
# MAIN PRINCIPAL
# ---------------------------------------------------------------------------

async def main():
    producer = AIOKafkaProducer(bootstrap_servers=BROKER)
    await producer.start()
    print(f"🩺 EV_CP_M {CP_ID} monitorizando {ENGINE_ADDR}, CENTRAL {CENTRAL_ADDR}")
    print(f"Broker Kafka: {BROKER}")
    print(f"Ubicación: {LOCATION}\n")

    try:
        # Ejecutar el menú y los heartbeats automáticos en paralelo
        await asyncio.gather(
            heartbeat_loop(producer),
            menu_monitor(producer)
        )
    finally:
        await producer.stop()

asyncio.run(main())
