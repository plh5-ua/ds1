# EV_CP_M.py
import asyncio
import json
import sys
import socket
import os
import requests
import time
import base64
import hashlib
from cryptography.hazmat.primitives.ciphers.aead import AESGCM


if len(sys.argv) < 7:
    print("Uso: python EV_CP_M.py <ip_engine:puerto> <ip_central:puerto> <ip_registry:puerto> <id_cp> <location> <price>")
    sys.exit(1)

ENGINE_ADDR = sys.argv[1]
CENTRAL_ADDR = sys.argv[2]
REGISTRY_ADDR = sys.argv[3]
CP_ID = sys.argv[4]
LOCATION = sys.argv[5].replace("_", " ")
PRICE = float(sys.argv[6])

ENGINE_IP, ENGINE_PORT = ENGINE_ADDR.split(":")
ENGINE_PORT = int(ENGINE_PORT)
CENTRAL_IP, CENTRAL_PORT = CENTRAL_ADDR.split(":")
CENTRAL_PORT = int(CENTRAL_PORT)
REGISTRY_IP, REGISTRY_PORT = REGISTRY_ADDR.split(":")
REGISTRY_PORT = int(REGISTRY_PORT)

REGISTRY_BASE = f"https://{REGISTRY_IP}:{REGISTRY_PORT}"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REGISTRY_VERIFY = os.path.join(BASE_DIR, "cert.pem")


CRED_FILE = f"cp_{CP_ID}_credential.json"
KEY_FILE  = f"cp_{CP_ID}_secretkey.json"

# --- estado heartbeats ---
HB_TASK: asyncio.Task | None = None
HB_STOP = asyncio.Event()

# -------------------------------------------------------------
# CIFRADO AES-GCM
# -------------------------------------------------------------
def load_secret_key_str() -> str:
    if not os.path.exists(KEY_FILE):
        raise RuntimeError("No hay secret_key. Autentica primero.")
    with open(KEY_FILE, "r", encoding="utf-8") as f:
        return json.load(f)["secret_key"]

def derive_aes_key(secret_key_str: str) -> bytes:
    return hashlib.sha256(secret_key_str.encode("utf-8")).digest()

def encrypt_to_secure_envelope(plain_obj: dict) -> dict:
    secret_key_str = load_secret_key_str()
    key = derive_aes_key(secret_key_str)
    aesgcm = AESGCM(key)

    nonce = os.urandom(12)
    ts = int(time.time())

    plaintext = json.dumps(plain_obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    aad = f"{CP_ID}|{ts}".encode("utf-8")

    ct = aesgcm.encrypt(nonce, plaintext, aad)

    return {
        "action": "SECURE",
        "cp_id": CP_ID,
        "ts": ts,
        "nonce": base64.b64encode(nonce).decode("ascii"),
        "ciphertext": base64.b64encode(ct).decode("ascii"),
    }

def decrypt_from_secure_envelope(envelope: dict) -> dict:
    """
    Recibe {"action":"SECURE","cp_id":..., "ts":..., "nonce":..., "ciphertext":...}
    y devuelve el JSON original (dict) ya descifrado.
    """
    cp_id = envelope.get("cp_id")
    if not cp_id:
        raise ValueError("NO_CP_ID")

    #comprobar que es para este cp
    if str(cp_id) != str(CP_ID):
        raise ValueError("CP_ID_MISMATCH")

    ts = int(envelope.get("ts") or 0)
    if ts <= 0:
        raise ValueError("BAD_TS")

    nonce_b64 = envelope.get("nonce") or ""
    ct_b64 = envelope.get("ciphertext") or ""
    if not nonce_b64 or not ct_b64:
        raise ValueError("MISSING_FIELDS")

    nonce = base64.b64decode(nonce_b64)
    ct = base64.b64decode(ct_b64)

    secret_key_str = load_secret_key_str()
    key = derive_aes_key(secret_key_str)
    aesgcm = AESGCM(key)

    # Tiene que coincidir con el AAD que usa Central al cifrar respuestas
    aad = f"{cp_id}|{ts}".encode("utf-8")

    pt = aesgcm.decrypt(nonce, ct, aad)
    return json.loads(pt.decode("utf-8"))


# -------------------------------------------------------------
# CENTRAL (socket)
# -------------------------------------------------------------
def _recv_all_with_timeout(sock: socket.socket, max_bytes: int = 65536) -> bytes:
    """
    Lee hasta que el servidor cierre o hasta agotar timeout.
    """
    chunks = []
    total = 0
    while True:
        try:
            part = sock.recv(4096)
        except socket.timeout:
            break
        if not part:
            break
        chunks.append(part)
        total += len(part)
        if total >= max_bytes:
            break
    return b"".join(chunks)

def send_to_central_and_recv(message, timeout=3) -> dict | str:
    action = (message.get("action") or "").upper()
    sent_secure = (action != "AUTH")

    if sent_secure:
        if not is_authenticated_local():
            raise RuntimeError("No autenticado: no puedo enviar cifrado.")
        message = encrypt_to_secure_envelope(message)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        s.connect((CENTRAL_IP, CENTRAL_PORT))
        s.sendall(json.dumps(message).encode("utf-8"))
        data = _recv_all_with_timeout(s, max_bytes=65536)
        if not data:
            return ""

    raw = data.decode("utf-8")

    # Si mandé cifrado, espero respuesta cifrada
    if sent_secure:
        env = json.loads(raw)
        if (env.get("action") or "").upper() != "SECURE":
            raise RuntimeError(f"Respuesta no cifrada inesperada: {raw}")
        return decrypt_from_secure_envelope(env)  # devuelve dict en claro

    # AUTH (en claro)
    return raw



# -------------------------------------------------------------
# REGISTRY (REST)
# -------------------------------------------------------------
def registry_alta(location: str):
    url = f"{REGISTRY_BASE}/cp/{CP_ID}"
    payload = {"cp_id": CP_ID, "location": location, "price": PRICE, "ip": ENGINE_IP}
    r = requests.put(url, json=payload, verify=REGISTRY_VERIFY, timeout=5)
    if r.status_code != 200:
        raise RuntimeError(f"ALTA falló: {r.status_code} {r.text}")
    data = r.json()
    cred = data["credential"]
    with open(CRED_FILE, "w", encoding="utf-8") as f:
        json.dump({"cp_id": CP_ID, "credential": cred}, f)
    print("Alta OK. Credential guardada en", CRED_FILE)

def registry_baja():
    url = f"{REGISTRY_BASE}/cp/{CP_ID}"
    r = requests.delete(url, verify=REGISTRY_VERIFY, timeout=5)
    if r.status_code != 200:
        raise RuntimeError(f"BAJA falló: {r.status_code} {r.text}")
    for fpath in (CRED_FILE, KEY_FILE):
        try:
            os.remove(fpath)
        except FileNotFoundError:
            pass
    print("Baja OK. Credenciales locales eliminadas.")

def load_credential() -> str:
    if not os.path.exists(CRED_FILE):
        raise RuntimeError("No hay credential. Primero haz DAR DE ALTA.")
    with open(CRED_FILE, "r", encoding="utf-8") as f:
        return json.load(f)["credential"]

def save_secret_key(secret_key: str):
    with open(KEY_FILE, "w", encoding="utf-8") as f:
        json.dump({"cp_id": CP_ID, "secret_key": secret_key}, f)
    print("🔐 Secret key guardada en", KEY_FILE)

def is_authenticated_local() -> bool:
    return os.path.exists(KEY_FILE)


# -------------------------------------------------------------
# ENGINE (socket)
# -------------------------------------------------------------
def send_id_to_engine():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(3)
            s.connect((ENGINE_IP, ENGINE_PORT))
            s.sendall(json.dumps({"cp_id": CP_ID, "location": LOCATION}).encode())
            ack = s.recv(16).decode().strip()
            return ack == "ACK"
    except Exception as e:
        print(f"Error al enviar ID al Engine: {e}")
        return False

def ping_engine():
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(2)
            s.connect((ENGINE_IP, ENGINE_PORT))
            s.sendall(b"HEARTBEAT")
            return s.recv(16).decode().strip()
    except:
        return None


# -------------------------------------------------------------
# Heartbeats (background)
# -------------------------------------------------------------
async def heartbeat_loop():
    try:
        while not HB_STOP.is_set():
            resp = ping_engine()
            health = "OK" if resp == "OK" else "KO"
            out = send_to_central_and_recv(
                {"action": "HEARTBEAT", "cp_id": CP_ID, "health": health, "ip" : ENGINE_IP},
                timeout=2
            )
            print(f"Heartbeat {CP_ID} ({health}) -> Central: {out}")
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        print("Heartbeats detenidos.")

def start_heartbeats():
    global HB_TASK
    if HB_TASK and not HB_TASK.done():
        print("Heartbeats ya están en marcha.")
        return
    HB_STOP.clear()
    HB_TASK = asyncio.create_task(heartbeat_loop())
    print("Heartbeats iniciados en background.")

async def stop_heartbeats():
    global HB_TASK
    if not HB_TASK or HB_TASK.done():
        print("ℹHeartbeats no están en marcha.")
        return
    HB_STOP.set()
    HB_TASK.cancel()
    try:
        await HB_TASK
    except asyncio.CancelledError:
        pass
    HB_TASK = None


# -------------------------------------------------------------
# MENÚ
# -------------------------------------------------------------
def print_menu():
    print("\n===== MENÚ CP_MONITOR =====")
    print("1) Dar de alta (Registry REST)")
    print("2) Dar de baja (Registry REST)")
    print("3) Autenticar (Central SOCKET)")
    print("4) Parar Heartbeats")
    print("5) Cambiar ubicación (UPDATE en Central)")
    print("0) Salir")
    print("===========================\n")

async def main():
    print(f"🩺 EV_CP_M {CP_ID} | Engine:{ENGINE_ADDR} | Central:{CENTRAL_ADDR} | Registry:{REGISTRY_ADDR}")

    current_location = LOCATION

    while True:
        print_menu()

        # input() bloquea: lo pasamos a un hilo para no bloquear asyncio
        op = (await asyncio.to_thread(input, "Opción: ")).strip()

        if op == "0":
            await stop_heartbeats()
            return

        elif op == "1":
            try:
                registry_alta(current_location)
            except Exception as e:
                print("Error:", e)

        elif op == "2":
            try:
                await stop_heartbeats()
                registry_baja()
            except Exception as e:
                print("Error:", e)

        elif op == "3":
            try:
                cred = load_credential()
                resp = send_to_central_and_recv(
                    {"action": "AUTH", "cp_id": CP_ID, "credential": cred, "ip": ENGINE_IP, "location": current_location, "price": PRICE},
                    timeout=3
                )
                if resp.startswith("DENIED"):
                    print("Central denegó:", resp)
                else:
                    data = json.loads(resp)
                    if data.get("ok"):
                        save_secret_key(data["secret_key"])
                        print("Autenticado en Central.")
                        if send_id_to_engine():
                            print("Engine ACK.")
                        else:
                            print("Engine no respondió ACK.")
                        if not is_authenticated_local():
                            print("No autenticado. Primero opción 3 (Autenticar).")
                            continue
                        start_heartbeats()
                    else:
                        print("Respuesta inesperada:", resp)
            except Exception as e:
                print("Excepción: ", e)

        elif op == "4":
            await stop_heartbeats()

        elif op == "5":
          try:
              if not is_authenticated_local():
                  print("No autenticado. Primero opción 3 (Autenticar).")
                  continue

              new_loc = (await asyncio.to_thread(input, "Nueva ubicación (ej: Oslo,NO): ")).strip()
              if not new_loc:
                  print("Ubicación vacía, cancelado.")
                  continue

              # Actualiza variable local para que a partir de ahora uses la nueva location en alta/auth/etc.
              current_location = new_loc.replace("_", " ")

              # Pídeselo a Central (irá cifrado automáticamente porque action != AUTH)
              resp = send_to_central_and_recv(
                  {"action": "UPDATE_LOCATION", "cp_id": CP_ID, "location": current_location , "ip": ENGINE_IP},
                  timeout=3
              )

              print("Central response:", resp)

              # (Opcional) si quieres que Engine también “se entere” (no es necesario para weather),
              # puedes reenviar {"cp_id", "location"} al engine:
              # send_id_to_engine()

          except Exception as e:
              print("Error:", e)


        else:
            print("Opción no válida.")

if __name__ == "__main__":
    asyncio.run(main())
