import asyncio, json, sys, socket

if len(sys.argv) < 6:
    print("Uso: python EV_CP_M.py <ip_engine:puerto> <ip_central:puerto> <id_cp> <location> <price>")
    sys.exit(1)

ENGINE_ADDR = sys.argv[1]
CENTRAL_ADDR = sys.argv[2]
CP_ID = sys.argv[3]
LOCATION = sys.argv[4].replace("_", " ")
PRICE = float(sys.argv[5])

ENGINE_IP, ENGINE_PORT = ENGINE_ADDR.split(":")
ENGINE_PORT = int(ENGINE_PORT)
CENTRAL_IP, CENTRAL_PORT = CENTRAL_ADDR.split(":")
CENTRAL_PORT = int(CENTRAL_PORT)

# -------------------------------------------------------------
# Comunicación con CENTRAL (por socket)
# -------------------------------------------------------------
def send_to_central(message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((CENTRAL_IP, CENTRAL_PORT))
            s.sendall(json.dumps(message).encode())
    except Exception as e:
        print(f"⚠️ Error enviando a CENTRAL: {e}")

# -------------------------------------------------------------
# Comunicación con ENGINE (por socket)
# -------------------------------------------------------------
def send_id_to_engine():
    """Envía ID y ubicación al Engine y espera ACK."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3)
            s.connect((ENGINE_IP, ENGINE_PORT))
            s.sendall(json.dumps({"cp_id": CP_ID, "location": LOCATION}).encode())
            ack = s.recv(16).decode().strip()
            return ack == "ACK"
    except Exception as e:
        print(f"❌ Error al enviar ID al Engine: {e}")
        return False

def ping_engine():
    """Envía heartbeat al Engine y recibe OK/KO."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((ENGINE_IP, ENGINE_PORT))
            s.sendall(b"HEARTBEAT")
            return s.recv(16).decode().strip()
    except:
        return None

# -------------------------------------------------------------
# Heartbeat loop
# -------------------------------------------------------------
async def heartbeat_loop():
    while True:
        resp = ping_engine()
        health = "OK" if resp == "OK" else "KO"
        send_to_central({
            "action": "HEARTBEAT",
            "cp_id": CP_ID,
            "health": health
        })
        print(f"❤️‍🔥 Heartbeat {CP_ID} ({health})")
        await asyncio.sleep(1)

# -------------------------------------------------------------
# MAIN
# -------------------------------------------------------------
async def main():
    print(f"🩺 EV_CP_M {CP_ID} iniciado | Engine: {ENGINE_ADDR} | Central: {CENTRAL_ADDR}")
    # 1️⃣ Registrar en CENTRAL
    send_to_central({
        "action": "REGISTER",
        "cp_id": CP_ID,
        "location": LOCATION,
        "price": PRICE
    })
    # 2️⃣ Enviar ID al Engine
    if send_id_to_engine():
        print(f"✅ Engine confirmó ACK para {CP_ID}. Iniciando heartbeats...")
        await heartbeat_loop()
    else:
        print("❌ No se recibió ACK del Engine. No se iniciarán heartbeats.")

asyncio.run(main())
