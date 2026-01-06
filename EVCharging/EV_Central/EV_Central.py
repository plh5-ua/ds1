"""
EV_Central — Sistema central de monitorización de puntos de recarga
Uso:
    python EV_Central.py <puerto_http> <ip_broker:puerto>

Ejemplo:
    python EV_Central.py 8080 127.0.0.1:9092
"""

import sys
import json
import asyncio
import sqlite3
import threading
import socket
from contextlib import closing
from typing import Dict, Any
from collections import deque
from datetime import datetime
from typing import Dict, Any
from typing import Dict, Any, Set, List


# --- FastAPI / WebSocket ---
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from requests import request
import uvicorn
from fastapi.responses import HTMLResponse
from pathlib import Path
from pydantic import BaseModel

# --- Kafka asíncrono ---
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

import os
import base64
import hashlib
import hmac
import secrets

#---- Cifras y autenticación ----
import time
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

PEPPER = os.getenv("EV_REGISTRY_PEPPER", "CHANGE_ME")  # MISMO que Registry

AUTHENTICATED_CPS = set()     # cp_id autenticados en CENTRAL
CP_SECRET_KEYS = {}           # cp_id -> secret_key (base64)

# ---------------------------------------------------------------------------
# Monitoreo de heartbeats
# ---------------------------------------------------------------------------
LAST_HEARTBEAT = {}
HEARTBEAT_TIMEOUT = 3.2  # segundos sin recibir heartbeat → DESCONECTADO
LAST_STATUS_SEEN = {}
# Última telemetría por CP para exponerla en /cp y en el panel
LAST_TELEMETRY: Dict[str, Dict[str, Any]] = {}

# --- Estado y buffers de CENTRAL ---
CENTRAL_STATUS = "OK"
LAST_MESSAGES = deque(maxlen=5)     # últimos 5 mensajes
RECENT_SESSIONS = deque(maxlen=50)  # histórico corto de inicios de sesión
MAIN_LOOP = None
# ---------------------------------------------------------------------------
# ARGUMENTOS DE EJECUCIÓN
# ---------------------------------------------------------------------------

if len(sys.argv) < 2:
    print("Uso: python EV_Central.py <puerto_http> <ip_broker:puerto>")
    sys.exit(1)

HTTP_PORT = int(sys.argv[1])
KAFKA_BOOTSTRAP = sys.argv[2]
SOCKET_PORT = 9000  # puerto para monitores

print("Iniciando EV_Central ...")
print(f"  • Puerto HTTP: {HTTP_PORT}")
print(f"  • Broker Kafka: {KAFKA_BOOTSTRAP}")
print(f"  • Puerto SOCKET monitores: {SOCKET_PORT}")

DB_PATH = "evcentral.db"


# ---------------------------------------------------------------------------
# BASE DE DATOS SQLITE
# ---------------------------------------------------------------------------

def get_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    # Usa el schema.sql con tablas: charging_points, sessions, events
    with open("schema.sql", "r", encoding="utf-8") as f:
        schema = f.read()
    with closing(get_db()) as con:
        con.executescript(schema)
        con.commit()

#------------------------------
#funciones parte 2 seguras
#------------------------------
SEEN = set()
SEEN_Q = deque(maxlen=2000)

def seen_before(key: str) -> bool:  ##evitamos MITM/replay attacks
    if key in SEEN:
        return True
    SEEN.add(key)
    SEEN_Q.append(key)
    if len(SEEN_Q) == SEEN_Q.maxlen:
        old = SEEN_Q.popleft()
        SEEN.discard(old)
    return False

def init_central_auth_tables():
    with closing(get_db()) as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS cp_registry_credentials (
            cp_id TEXT PRIMARY KEY,
            cred_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            issued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            revoked INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS cp_central_keys (
            cp_id TEXT PRIMARY KEY,
            secret_key TEXT NOT NULL,
            issued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            revoked INTEGER NOT NULL DEFAULT 0
        );
        """)
        con.commit()
def revoke_secret_key(cp_id: str):
    with closing(get_db()) as con:
        con.execute(
            "UPDATE cp_central_keys SET revoked=1 WHERE cp_id=?",
            (cp_id,)
        )
        con.commit()


        con.commit()
def revoke_cp_keys_everywhere(cp_id: str):
    revoke_secret_key(cp_id)

    CP_SECRET_KEYS.pop(cp_id, None)
    AUTHENTICATED_CPS.discard(cp_id)
    update_cp(cp_id, "DESCONECTADO")

def hash_cred(cred_plain: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        (cred_plain + PEPPER).encode("utf-8"),
        salt.encode("utf-8"),
        200_000
    )
    return base64.urlsafe_b64encode(dk).decode().rstrip("=")

def verify_registry_credential(cp_id: str, credential: str) -> bool:
    with closing(get_db()) as con:
        row = con.execute(
            "SELECT cred_hash, salt, revoked FROM cp_registry_credentials WHERE cp_id=?",
            (cp_id,)
        ).fetchone()
        if not row or int(row["revoked"]) == 1:
            return False
        expected = row["cred_hash"]
        salt = row["salt"]
        got = hash_cred(credential, salt)
        return hmac.compare_digest(expected, got)

def upsert_secret_key(cp_id: str, secret_key: str):
    with closing(get_db()) as con:
        con.execute("""
            INSERT INTO cp_central_keys(cp_id, secret_key, revoked)
            VALUES (?,?,0)
            ON CONFLICT(cp_id) DO UPDATE SET
                secret_key=excluded.secret_key,
                issued_at=CURRENT_TIMESTAMP,
                revoked=0
        """, (cp_id, secret_key))
        con.commit()

#AUDIT LOG
def insert_audit_log(ip_auditor: str, name_auditor: str, action: str, details: str = None):
    with closing(get_db()) as con:
        con.execute("""
            INSERT INTO audit_log(ip_auditor, name_auditor, action, details)
            VALUES (?,?,?,?)
        """, (ip_auditor, name_auditor, action, details))
        con.commit()

#funciones p1 db
def insert_cp(cp_id: str, location: str, price: float = 0.3):
    """Upsert: si existe actualiza location/price; si no, inserta."""
    with closing(get_db()) as con:
        cur = con.cursor()
        row = cur.execute("SELECT id FROM charging_points WHERE id=?", (cp_id,)).fetchone()
        if row:
            cur.execute(
                "UPDATE charging_points SET location=?, price_eur_kwh=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                (location, price, cp_id),
            )
        else:
            cur.execute(
                "INSERT INTO charging_points(id, location, price_eur_kwh) VALUES (?, ?, ?)",
                (cp_id, location, price),
            )
        con.commit()

def is_suministrando_cp(cp_id: str) -> bool:
    with closing(get_db()) as con:
        cur = con.cursor()
        row = cur.execute("SELECT status FROM charging_points WHERE id=?", (cp_id,)).fetchone()
    if row is None:
        return False
    status = row[0]
    return status.upper() == "SUMINISTRANDO"

def update_cp(cp_id: str, status: str):
    with closing(get_db()) as con:
        cur = con.cursor()
        cur.execute(
            "UPDATE charging_points SET status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (status, cp_id),
        )
        con.commit()


def list_cps():
    with closing(get_db()) as con:
        rows = con.execute("SELECT * FROM charging_points").fetchall()
        return [dict(r) for r in rows]

def mark_all_cps_disconnected():
    """Marca todos los puntos como DESCONECTADOS al iniciar CENTRAL."""
    with closing(get_db()) as con:
        cur = con.cursor()
        cur.execute("UPDATE charging_points SET status='DESCONECTADO', updated_at=CURRENT_TIMESTAMP")
        con.commit()
    print("Todos los puntos de recarga marcados como DESCONECTADOS al iniciar CENTRAL.")

def get_cp_from_db(cp_id):
    with closing(get_db()) as con:
        row = con.execute("SELECT * FROM charging_points WHERE id=?", (cp_id,)).fetchone()
        return dict(row) if row else None


def db_get_cp(con, cp_id):
    return con.execute(
        "SELECT id, status, price_eur_kwh FROM charging_points WHERE id=?",
        (cp_id,)
    ).fetchone()

def get_cp_ip_from_db(con, cp_id):
    with closing(get_db()) as con:
        row = con.execute("SELECT * FROM charging_points WHERE id=?", (cp_id,)).fetchone()
        return dict(row) if row else None

def log_event(cp_id, driver_id, etype, payload):
    with closing(get_db()) as con:
        con.execute(
            "INSERT INTO events (cp_id, driver_id, type, payload) VALUES (?,?,?,?)",
            (cp_id, driver_id, etype, json.dumps(payload))
        )
        con.commit()


def start_session(cp_id, driver_id, price_eur_kwh):
    with closing(get_db()) as con:
        cur = con.cursor()
        cur.execute(
            "INSERT INTO sessions (cp_id, driver_id, price_eur_kwh, status) VALUES (?,?,?, 'RUNNING')",
            (cp_id, driver_id, price_eur_kwh)
        )
        sid = cur.lastrowid
        con.commit()
        return sid


def update_session_progress(session_id, kwh, amount_eur):
    with closing(get_db()) as con:
        con.execute(
            "UPDATE sessions SET kwh=?, amount_eur=? WHERE id=?",
            (kwh, amount_eur, session_id)
        )
        con.commit()


def end_session(session_id, kwh, amount_eur, ended_status="ENDED"):
    with closing(get_db()) as con:
        con.execute(
            "UPDATE sessions SET ended_at=CURRENT_TIMESTAMP, kwh=?, amount_eur=?, status=? WHERE id=?",
            (kwh, amount_eur, ended_status, session_id)
        )
        con.commit()


# ---------------------------------------------------------------------------
# AUTH y cifrado
# ---------------------------------------------------------------------------

def derive_aes_key(secret_key_str: str) -> bytes:
    # 32 bytes -> AES-256
    return hashlib.sha256(secret_key_str.encode("utf-8")).digest()

def decrypt_secure_envelope(envelope: dict) -> dict:
    cp_id = envelope.get("cp_id")
    if not cp_id:
        raise ValueError("NO_CP_ID")

    # 1) comprobar que hay clave para ese CP
    secret_key_str = CP_SECRET_KEYS.get(cp_id)
    if not secret_key_str:
        raise ValueError("NO_KEY_FOR_CP")

    # 2) anti-replay simple con timestamp
    ts = int(envelope.get("ts") or 0)
    now = int(time.time())
    if ts <= 0 or abs(now - ts) > 20:
        raise ValueError("STALE_OR_BAD_TS")

    # 3) descifrar
    nonce_b64 = envelope.get("nonce") or ""
    ct_b64 = envelope.get("ciphertext") or ""

    nonce = base64.b64decode(nonce_b64)
    ct = base64.b64decode(ct_b64)

    key = derive_aes_key(secret_key_str)
    aesgcm = AESGCM(key)

    uniq = f"{ts}:{envelope.get('nonce')}"
    if seen_before(uniq):
        raise ValueError("REPLAY_DETECTED")

    # AAD: ata cp_id y ts a la autenticidad del mensaje
    aad = f"{cp_id}|{ts}".encode("utf-8")

    pt = aesgcm.decrypt(nonce, ct, aad)
    return json.loads(pt.decode("utf-8"))
def load_secret_key_from_db(cp_id: str) -> str | None:
    with closing(get_db()) as con:
        row = con.execute(
            "SELECT secret_key, revoked FROM cp_central_keys WHERE cp_id=?",
            (cp_id,)
        ).fetchone()
        if not row:
            return None
        if int(row["revoked"]) == 1:
            return None
        return row["secret_key"]

def encrypt_secure_envelope(cp_id: str, plaintext_msg: dict) -> dict:
    secret_key_str = CP_SECRET_KEYS.get(cp_id)
    if not secret_key_str:
        secret_key_str = load_secret_key_from_db(cp_id)
        if not secret_key_str:
            raise ValueError("NO_KEY_FOR_CP_OR_REVOKED")
        CP_SECRET_KEYS[cp_id] = secret_key_str

    ts = int(time.time())

    key = derive_aes_key(secret_key_str)
    aesgcm = AESGCM(key)

    nonce = os.urandom(12)  # 96 bits recomendado para GCM

    aad = f"{cp_id}|{ts}".encode("utf-8")
    pt = json.dumps(plaintext_msg, separators=(",", ":")).encode("utf-8")
    ct = aesgcm.encrypt(nonce, pt, aad)

    return {
        "action": "SECURE",
        "cp_id": cp_id,
        "ts": ts,
        "nonce": base64.b64encode(nonce).decode("utf-8"),
        "ciphertext": base64.b64encode(ct).decode("utf-8"),
    }

# ---------------------------------------------------------------------------
# FASTAPI + PANEL
# ---------------------------------------------------------------------------

app = FastAPI(title="EV_Central")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def index():
    return Path("static/index.html").read_text(encoding="utf-8")


PANEL_CLIENTS = set()


@app.get("/cp")
def api_list_cps():
    cps = list_cps()
    for cp in cps:
        lt = LAST_TELEMETRY.get(cp["id"])
        if lt:
            cp["kwh_total"] = lt.get("kwh_total", 0.0)
            cp["eur_total"] = lt.get("eur_total", 0.0)
            cp["driver_id"] = lt.get("driver_id")
        else:
            cp["kwh_total"] = None
            cp["eur_total"] = None
            cp["driver_id"] = None
    return cps


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    PANEL_CLIENTS.add(ws)
    print("Cliente conectado al panel")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        PANEL_CLIENTS.discard(ws)
        print("Cliente desconectado")
@app.get("/central/summary")
def api_central_summary():
    return {
        "status": CENTRAL_STATUS,
        "messages": list(LAST_MESSAGES),
        "sessions": list(RECENT_SESSIONS)
    }

from fastapi import Body
@app.post("/central/state")
async def api_set_central_state(payload: Dict[str, Any] = Body(...)):
    global CENTRAL_STATUS
    status = (payload.get("status") or "OK").upper()
    if status not in ("OK", "STOP"):
        return {"ok": False, "error": "status debe ser OK o STOP"}
    CENTRAL_STATUS = status
    await notify_central_state()
    log_central_msg("CENTRAL_STATE", {"status": CENTRAL_STATUS})
    return {"ok": True, "status": CENTRAL_STATUS}

@app.on_event("shutdown")
async def shutdown_event():
    try:
        print("Cerrando conexión Kafka...")
        await kafka_consumer.stop()
        await kafka_producer.stop()
        print("Kafka cerrado correctamente.")
    except Exception as e:
        print("Error cerrando Kafka:", e)

@app.post("/central/revoke_key/{cp_id}")
async def api_revoke_key(cp_id: str):
    cp_id = cp_id.strip()

    cp = get_cp_from_db(cp_id)
    if not cp:
        raise HTTPException(status_code=404, detail="CP no existe")

    if cp_id in ACTIVE_SESSIONS:
        await force_close_session(cp_id, "KEY_REVOKED")

    revoke_cp_keys_everywhere(cp_id)

    log_central_msg("KEY_REVOKED", {"cp_id": cp_id})
    await notify_panel({"type": "key.revoked", "cp_id": cp_id})

    return {"ok": True, "cp_id": cp_id, "revoked": True}

async def notify_panel(event: Dict[str, Any]):
    dead = []
    for ws in list(PANEL_CLIENTS):
        try:
            await ws.send_text(json.dumps(event))
        except Exception:
            dead.append(ws)
    for ws in dead:
        PANEL_CLIENTS.discard(ws)
def now_iso():
    return datetime.now().isoformat(timespec="seconds")

def log_central_msg(msg_type: str, detail: dict):
    item = {"ts": now_iso(), "msg_type": msg_type, "detail": detail}
    LAST_MESSAGES.append(item)
    loop = MAIN_LOOP or asyncio.get_running_loop()
    asyncio.run_coroutine_threadsafe(
        notify_panel({"type": "central.msg", **item}),
        loop
    )

async def notify_central_state():
    await notify_panel({"type": "central.state", "status": CENTRAL_STATUS})

class Command(BaseModel):
    action: str           # "STOP" | "RESUME"
    cp_id: str = "ALL"    # puedes enviar "ALL" o un ID concreto

class WeatherUpdate(BaseModel):
    location: str
    temp_c: float
    alert: bool

@app.post("/weather/alert")
async def api_weather_alert(u: WeatherUpdate):
    location = (u.location or "").strip()
    if not location:
        raise HTTPException(status_code=400, detail="location vacío")

    loc_key = norm_loc(location)

    WEATHER_STATE[loc_key] = {
        "location": location,
        "temp_c": float(u.temp_c),
        "alert": bool(u.alert),
        "ts": now_iso()
    }

    # Avisar al panel
    await notify_panel({"type": "weather.update", **WEATHER_STATE[loc_key]})
    log_central_msg("WEATHER_UPDATE", WEATHER_STATE[loc_key])

    # Buscar CPs en esa localización
    cps = list_cps()
    cp_ids = [cp["id"] for cp in cps if norm_loc(cp.get("location")) == loc_key]

    if not cp_ids:
        return {"ok": True, "note": f"No hay CPs con location={location}", "state": WEATHER_STATE[loc_key]}

    # ALERTA ON: parar CPs (pero si hay sesión, parar al terminar)
    if u.alert:
        for cp_id in cp_ids:
            WEATHER_DISABLED_CPS.add(cp_id)

            if cp_id in ACTIVE_SESSIONS:
                # está cargando → NO cortamos
                WEATHER_PENDING_STOP.add(cp_id)
                await notify_panel({"type": "weather.stop_pending", "cp_id": cp_id, "location": location})
                log_central_msg("WEATHER_STOP_PENDING", {"cp_id": cp_id, "location": location})
            else:
                # no está cargando → STOP inmediato
                await send_cp_command("STOP", cp_id, source="weather.alert_on")
                update_cp(cp_id, "PARADO")
                await notify_panel({"type": "status", "cp_id": cp_id, "status": "PARADO"})
                log_central_msg("WEATHER_STOP", {"cp_id": cp_id, "location": location})

    # ALERTA OFF: reanudar los CPs parados por clima / cancelar pendientes
    else:
        for cp_id in cp_ids:
            WEATHER_PENDING_STOP.discard(cp_id)

            if cp_id in WEATHER_DISABLED_CPS:
                WEATHER_DISABLED_CPS.discard(cp_id)
                await send_cp_command("RESUME", cp_id, source="weather.alert_off")
                log_central_msg("WEATHER_RESUME", {"cp_id": cp_id, "location": location})

    return {
        "ok": True,
        "state": WEATHER_STATE[loc_key],
        "affected_cps": cp_ids,
        "disabled_cps": sorted(WEATHER_DISABLED_CPS),
        "pending_stop": sorted(WEATHER_PENDING_STOP),
    }

@app.get("/weather/state")
def api_weather_state():
    return {
        "by_location": WEATHER_STATE,
        "disabled_cps": sorted(WEATHER_DISABLED_CPS),
        "pending_stop": sorted(WEATHER_PENDING_STOP),
    }


@app.post("/command")
async def send_command(cmd: Command):
    payload = {"action": cmd.action.upper(), "cp_id": cmd.cp_id}
    await kafka_producer.send_and_wait("central.command", json.dumps(payload).encode())
    return {"status": "ok", "sent": payload}

def list_active_sessions():
    with closing(get_db()) as con:
        rows = con.execute("""
            SELECT id as session_id, cp_id, driver_id, datetime(started_at) as ts, status
            FROM sessions
            WHERE status='RUNNING'
            ORDER BY started_at DESC
            LIMIT 200
        """).fetchall()
        return [dict(r) for r in rows]

@app.get("/sessions/active")
def api_sessions_active():
    return list_active_sessions()


# ---------------------------------------------------------------------------
# SOCKET SERVER (para MONITORES)
# ---------------------------------------------------------------------------

import threading
import socket
import json

def monitor_socket_server(loop):
    """
    Recibe registros y heartbeats desde monitores vía socket (hilo dedicado).
    Atiende cada conexión entrante en un hilo corto para evitar rechazos.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", SOCKET_PORT))
    srv.listen(128)  # backlog alto para ráfagas
    print(f"Escuchando monitores en puerto {SOCKET_PORT}...")

    def handle_conn(conn, addr):
        try:
            # tiempo de espera razonable por si llega la cabecera troceada
            conn.settimeout(2.0)
            data = conn.recv(4096)
            if not data:
                return

            # Compatibilidad texto plano
            if data == b"PING":
                try: conn.sendall(b"OK")
                except Exception: pass
                return

            # JSON
            try:
                msg = json.loads(data.decode())
            except Exception:
                try: conn.sendall(b"NACK")
                except Exception: pass
                return

            action = (msg.get("action") or "").upper()
            cp_id  = msg.get("cp_id")

            came_secure = False

            # Si llega cifrado, lo desciframos y reemplazamos msg/action/cp_id
            if action == "SECURE":
                try:
                    msg = decrypt_secure_envelope(msg)
                except Exception:
                    try: conn.sendall(b"DENIED:BAD_SECURE_MESSAGE")
                    except: pass
                    return
                came_secure = True
                action = (msg.get("action") or "").upper()
                cp_id  = msg.get("cp_id")

            if action == "AUTH":
                credential = (msg.get("credential") or "").strip()

                # 1) El CP debe existir en BD (si no, no está dado de alta)
                cp_row = get_cp_from_db(cp_id)
                if not cp_row:
                    try:
                        conn.sendall(b"DENIED:NOT_REGISTERED")
                        insert_audit_log(msg.get("ip") or "unknown", "CP_M_"+cp_id, "AUTH_FAIL", "CP no registrado en CENTRAL por falta de alta previa")
                        log_central_msg("AUTH_FAIL", {"cp_id": cp_id, "reason": "NOT_REGISTERED"})
                    except: pass
                    return

                # 2) Validar credencial contra Registry
                if not verify_registry_credential(cp_id, credential):
                    try:
                        conn.sendall(b"DENIED:BAD_CREDENTIAL")
                        insert_audit_log(msg.get("ip") or "unknown", "CP_M_"+cp_id, "AUTH_FAIL", "CP no registrado en CENTRAL por credencial inválida")
                        log_central_msg("AUTH_FAIL", {"cp_id": cp_id, "reason": "BAD_CREDENTIAL"})
                    except: pass
                    return

                # 3) OK -> generar secret_key única por CP
                secret_key = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
                CP_SECRET_KEYS[cp_id] = secret_key
                upsert_secret_key(cp_id, secret_key)
                insert_audit_log(msg.get("ip") or "unknown", "CP_M_"+cp_id, "AUTH_SUCCESS", "CP autenticado correctamente en CENTRAL")

                location = msg.get("location")
                price    = msg.get("price")
                log_central_msg("AUTH_SUCCESS", {"cp_id": cp_id, "location": location, "price": price})
                AUTHENTICATED_CPS.add(cp_id)

                resp = {"ok": True, "cp_id": cp_id, "secret_key": secret_key}
                try: conn.sendall(json.dumps(resp).encode())
                except: pass
                return
            if action == "REGISTER":
                if not came_secure:
                    try: conn.sendall(b"DENIED:REQUIRE_SECURE")
                    except: pass
                    return
                location = msg.get("location", "Desconocida")
                price    = float(msg.get("price", 0.30))
                insert_cp(cp_id, location, price)
                cp_data = get_cp_from_db(cp_id)
                print(f"CP registrado desde monitor: {cp_id} ({location}, {price} €/kWh)")
                try:
                    if came_secure:
                        resp_msg = {"ok": True, "action": "REGISTER_ACK"}
                        conn.sendall(json.dumps(encrypt_secure_envelope(cp_id, resp_msg)).encode("utf-8"))
                    else:
                        conn.sendall(b"ACK")
                except Exception: pass
                # notificar panel desde el loop ASYNC
                asyncio.run_coroutine_threadsafe(
                    notify_panel({"type": "register", **cp_data}), loop
                )
                log_central_msg("REGISTRO_CP", {"cp_id": cp_id, "location": location, "price": price})

            elif action == "HEARTBEAT":
                if not came_secure:
                    try:
                        conn.sendall(b"DENIED:REQUIRE_SECURE")
                    except: pass
                    return
                if cp_id not in AUTHENTICATED_CPS:
                    try:
                        conn.sendall(b"DENIED:NOT_AUTH")
                        insert_audit_log(msg.get("ip") or "unknown", "CP_M_"+cp_id, "HEARTBEAT_FAIL", "CP no autenticado intentando enviar heartbeat")
                    except: pass
                    return

                LAST_HEARTBEAT[cp_id] = loop.time()
                health = (msg.get("health") or "KO").upper()
                new_status = "ACTIVADO" if health == "OK" else "AVERIA"

                # Estado previo (preferimos el último visto por heartbeat; si no, BD)
                try:
                    db_row = get_cp_from_db(cp_id) or {}
                    db_status = (db_row.get("status") or "DESCONECTADO").upper()
                except Exception:
                    db_status = "DESCONECTADO"

                prev_status = LAST_STATUS_SEEN.get(cp_id, db_status)

                # Si entra en KO mientras suministra → STOP inmediato
                if health == "KO" and db_status == "SUMINISTRANDO":
                    row = get_cp_from_db(cp_id)

                    insert_audit_log(msg.get("ip") or row.get("ip") or "unknown", "CP_M_"+cp_id, "FORCE_STOP", "CP en AVERIA mientras suministraba, forzando STOP")
                    log_central_msg("FORCE_STOP", {"cp_id": cp_id, "reason": "CP en AVERIA mientras suministraba, forzando STOP"})
                    asyncio.run_coroutine_threadsafe(
                        kafka_producer.send_and_wait(
                            "central.command",
                            json.dumps({"action": "STOP", "cp_id": cp_id}).encode()
                        ),
                        loop
                    )
                    asyncio.run_coroutine_threadsafe(
                        force_close_session(cp_id, "FAULT"),
                        loop
                    )

                # No machacar PARADO/DESCONECTADO con ACTIVADO solo por heartbeat
                if new_status == "ACTIVADO" and db_status in {"PARADO", "DESCONECTADO"}:
                    try:
                        if came_secure:
                            resp_msg = {
                                "ok": True,
                                "action": "HEARTBEAT_ACK",
                                "health": health ## mandamos health para que no cambie el estado
                                }
                            conn.sendall(json.dumps(encrypt_secure_envelope(cp_id, resp_msg)).encode("utf-8"))
                    except Exception:
                        pass
                    LAST_STATUS_SEEN[cp_id] = prev_status
                    return

                 # Evitar pasar a ACTIVADO si ya está suministrando
                if not (new_status == "ACTIVADO" and is_suministrando_cp(cp_id)):
                    # --- LOG de transición (una sola vez por cambio) ---
                    if prev_status != new_status:
                        if prev_status == "AVERIA" and new_status == "ACTIVADO":
                            insert_audit_log(msg.get("ip") or "unknown", "CP_E_"+cp_id, "FAULT_FIXED", "CP en AVERIA -> ACTIVADO")
                            log_central_msg("AVERIA SOLUCIONADA", {"cp_id": cp_id, "from": prev_status, "to": new_status})
                        elif prev_status == "ACTIVADO" and new_status == "AVERIA":
                            insert_audit_log(msg.get("ip") or "unknown", "CP_E_"+cp_id, "FAULT_ENGINE", "CP en AVERIA")
                            log_central_msg("AVERIA", {"cp_id": cp_id, "from": prev_status, "to": new_status})

                    # Persistir y notificar
                    update_cp(cp_id, new_status)
                    cp_data = get_cp_from_db(cp_id) or {"id": cp_id, "status": new_status}
                    cp_data["status"] = new_status
                    asyncio.run_coroutine_threadsafe(
                        notify_panel({"type": "heartbeat", **cp_data}), loop,
                    )
                    # Actualiza el último estado visto
                    LAST_STATUS_SEEN[cp_id] = new_status
                else:
                    # Si seguimos suministrando, no cambiamos a ACTIVADO, pero devolvemos OK
                    LAST_STATUS_SEEN[cp_id] = db_status

                try:
                    if came_secure:
                        resp_msg = {
                            "ok": True,
                            "action": "HEARTBEAT_ACK",
                            "health": health
                            }
                        conn.sendall(json.dumps(encrypt_secure_envelope(cp_id, resp_msg)).encode("utf-8"))
                except Exception:
                    pass

            else:
                try: conn.sendall(b"NACK")
                except Exception as e:
                    print(f"Error enviando NACK a {addr}: {e}")
                finally:
                    pass
        except Exception as e:
            print(f"Error procesando monitor desde {addr}: {e}")
        finally:
            try: conn.close()
            except Exception: pass

    # Bucle de aceptación: lanzar un hilo por conexión
    while True:
        conn, addr = srv.accept()
        threading.Thread(target=handle_conn, args=(conn, addr), daemon=True).start()

async def monitor_disconnections():
    """ Marca CPs como DESCONECTADOS si no envían heartbeats recientes."""
    while True:
        now = asyncio.get_running_loop().time()
        for cp_id, last in list(LAST_HEARTBEAT.items()):
            if now - last > HEARTBEAT_TIMEOUT:
                print(f"CP {cp_id} no ha mandado heartbeat en {HEARTBEAT_TIMEOUT}s → DESCONECTADO")

                # Si estaba suministrando, cierra sesión antes de actualizar estado
                if cp_id in ACTIVE_SESSIONS:
                    await force_close_session(cp_id, "DISCONNECTED")

                update_cp(cp_id, "DESCONECTADO")
                cp_data = get_cp_from_db(cp_id)
                if cp_data:
                    cp_data["status"] = "DESCONECTADO"
                    await notify_panel({"type": "heartbeat", **cp_data})
                log_central_msg("DISCONNECTED", {"cp_id": cp_id, "since_sec": HEARTBEAT_TIMEOUT})

                row = get_cp_from_db(cp_id)
                insert_audit_log(row.get("ip") or "unknown", "CP_E_"+cp_id, "DISCONNECTED", f"CP no envió heartbeats en tiempo, since_sec: {HEARTBEAT_TIMEOUT}")
                del LAST_HEARTBEAT[cp_id]


        await asyncio.sleep(2)

# ---------------------------------------------------------------------------
# KAFKA (Engines y Drivers)
# ---------------------------------------------------------------------------

kafka_consumer = None
kafka_producer = None

# Sesiones activas por CP (mapeo en memoria)
# cp_id -> {"driver_id":..., "request_id":..., "session_id":...}
ACTIVE_SESSIONS: Dict[str, Dict[str, Any]] = {}

# ---------------------------------------------------------------------------
# WEATHER (EV_W → CENTRAL)
# ---------------------------------------------------------------------------

def norm_loc(s: str) -> str:
    return (s or "").strip().casefold()

# key = location normalizada
WEATHER_STATE: Dict[str, Dict[str, Any]] = {}   # loc_key -> {"location","temp_c","alert","ts"}

# CPs que hemos parado por clima (para luego reanudarlos)
WEATHER_DISABLED_CPS: Set[str] = set()

# CPs que estaban cargando cuando llegó la alerta:
# NO los paramos en ese momento; los paramos justo al terminar sesión
WEATHER_PENDING_STOP: Set[str] = set()


async def force_close_session(cp_id: str, reason_code: str):
    """
    Cierra en CENTRAL la sesión activa del cp_id (si existe), usando la última
    telemetría conocida. Envía ticket al driver, actualiza panel y deja todo limpio.
    """
    sess = ACTIVE_SESSIONS.pop(cp_id, None)
    if not sess:
        return

    # Última lectura conocida (si no hubo, 0.0)
    lt = LAST_TELEMETRY.pop(cp_id, {}) or {}
    kwh_final = float(lt.get("kwh_total") or 0.0)
    eur_final = float(lt.get("eur_total") or 0.0)

    # Info del CP para ticket
    cp_row = get_cp_from_db(cp_id) or {}
    location = cp_row.get("location")
    unit_price = cp_row.get("price_eur_kwh")
    ip = cp_row.get("ip")

    # Cierra en BD con el código indicado
    end_session(sess["session_id"], kwh_final, eur_final, ended_status=reason_code)

    # Ticket al driver
    await kafka_producer.send_and_wait("driver.update", json.dumps({
        "driver_id": sess["driver_id"],
        "request_id": sess["request_id"],
        "status": "FINISHED",
        "message": f"Servicio finalizado en {cp_id}",
        "summary": {
            "cp_id": cp_id,
            "location": location,
            "price_eur_kwh": unit_price,
            "kwh": kwh_final,
            "amount_eur": eur_final,
            "reason": reason_code
        }
    }).encode())

    # Log + panel
    log_central_msg("SUMINISTRO_FINALIZADO", {
        "cp_id": cp_id, "reason": reason_code,
        "driver_id": sess["driver_id"], "kwh": kwh_final, "amount_eur": eur_final
    })

    insert_audit_log(ip or "unknown", "CP_E_"+cp_id, "SESSION_ENDED", f"Sesión {sess['session_id']} cerrada por {reason_code}, kwh: {kwh_final}, eur: {eur_final}")

    await notify_panel({
        "type": "session.ended",
        "ts": now_iso(),
        "cp_id": cp_id,
        "driver_id": sess["driver_id"],
        "session_id": sess["session_id"],
        "kwh": kwh_final,
        "amount_eur": eur_final,
        "reason": reason_code
    })

async def wait_kafka_ready(timeout_sec: float = 3.0) -> bool:
    start = time.monotonic()
    while kafka_producer is None:
        if time.monotonic() - start > timeout_sec:
            return False
        await asyncio.sleep(0.05)
    return True

async def send_cp_command(action: str, cp_id: str, source: str = "weather") -> bool:
    if not await wait_kafka_ready():
        # Para background: log y ya
        log_central_msg("KAFKA_NOT_READY", {"where": "send_cp_command", "cp_id": cp_id, "action": action})
        return False

    payload = {"action": action.upper(), "cp_id": cp_id}
    try:
        await kafka_producer.send_and_wait("central.command", json.dumps(payload).encode())
    except Exception as e:
        log_central_msg("KAFKA_SEND_ERROR", {"cp_id": cp_id, "action": action, "err": str(e)})
        return False

    await notify_panel({"type": "command.sent", "ts": now_iso(), "source": source, **payload})
    log_central_msg("COMMAND_SENT", {"source": source, **payload})
    return True



async def consume_kafka():
    global kafka_consumer
    kafka_consumer = AIOKafkaConsumer(
        "cp.status",
        "cp.telemetry",
        "cp.session_ended",
        "driver.request",
        "engine.start_manual",
        "engine.reject",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="central-consumer",
    )
    await kafka_consumer.start()

    # Espera a que producer esté listo
    global kafka_producer
    while kafka_producer is None:
        await asyncio.sleep(0.05)

    try:
        async for msg in kafka_consumer:
            topic = msg.topic
            data = msg.value
            cp_id = data.get("cp_id")
            location = data.get("location")
            price = data.get("kwh", 0.30)

            if topic == "cp.status":
                status = (data.get("status") or "ACTIVADO").upper()

                # Si CENTRAL está en STOP, no aceptar transiciones a ACTIVADO
                if CENTRAL_STATUS == "STOP" and status == "ACTIVADO":
                    # Mantener PARADO (o el estado actual si es más restrictivo)
                    row = get_cp_from_db(cp_id) or {}
                    cur = (row.get("status") or "DESCONECTADO").upper()
                    status = "PARADO" if cur not in {"AVERIA", "DESCONECTADO"} else cur
                    log_central_msg("STOP_GLOBAL_FILTER",
                                    {"cp_id": cp_id, "kept": status, "ignored": "ACTIVADO"})
                    broker_ip, _ = KAFKA_BOOTSTRAP.split(":")
                    insert_audit_log(broker_ip, "CENTRAL", "STOP_GLOBAL_FILTER", "STOP GLOBAL activo")

                # Si estaba suministrando y el nuevo estado no permite suministro → cerrar
                #if status in {"AVERIA", "PARADO", "DESCONECTADO"} and cp_id in ACTIVE_SESSIONS:
                 #   reason_code = "FAULT" if status == "AVERIA" else ("DISCONNECTED" if status == "DESCONECTADO" else "ABORTED")
                  #  await force_close_session(cp_id, reason_code)

                update_cp(cp_id, status)
                await notify_panel({"type": "status", "cp_id": cp_id, "status": status})



            elif topic == "driver.request":
                req_cp = data["cp_id"]; req_driver = data["driver_id"]; req_id = data["request_id"]; ip = data["ip"]
                print(f"[driver.request] {data}")

                # Ocupado
                if req_cp in ACTIVE_SESSIONS:
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": req_driver, "request_id": req_id,
                        "status": "DENIED", "message": f"CP {req_cp} ocupado: hay una sesión en curso"
                    }).encode())
                    continue

                # 2) Existe y estado
                with closing(get_db()) as con:
                    row = db_get_cp(con, req_cp)
                if not row:
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": req_driver, "request_id": req_id,
                        "status": "DENIED", "message": f"CP {req_cp} no registrado en CENTRAL"
                    }).encode())
                    continue

                cp_status = row["status"]
                if cp_status != "ACTIVADO":
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": req_driver, "request_id": req_id,
                        "status": "DENIED", "message": f"CP {req_cp} no disponible (estado {cp_status})"
                    }).encode())
                    continue

                # 3) Crear sesión RUNNING
                price = row["price_eur_kwh"]
                session_id = start_session(req_cp, req_driver, price)
                ACTIVE_SESSIONS[req_cp] = {
                    "driver_id": req_driver,
                    "request_id": req_id,
                    "session_id": session_id,
                    "notified_started": False
                }
                log_event(req_cp, req_driver, "AUTH", {"request_id": req_id})

                started_item = {
                    "ts": now_iso(),           # helper que devuelve fecha/hora
                    "cp_id": req_cp,
                    "driver_id": req_driver,
                    "session_id": session_id
                }
                RECENT_SESSIONS.appendleft(started_item)                     # buffer en memoria
                log_central_msg("SUMINISTRO_SOLICITADO", {                         # mensaje central
                    "cp_id": req_cp, "driver_id": req_driver, "session_id": session_id
                })
                insert_audit_log(ip, "DRIVER_"+req_driver, "SESSION_STARTED", f"Sesión {session_id} iniciada en CP {req_cp}")

                # 4) Avisar a Engine y Driver
                await kafka_producer.send_and_wait("central.authorize", json.dumps({
                    "cp_id": req_cp, "driver_id": req_driver, "request_id": req_id
                }).encode())
                await kafka_producer.send_and_wait("driver.update", json.dumps({
                    "driver_id": req_driver, "request_id": req_id,
                    "status": "AUTHORIZED", "message": f"Autorizado en {req_cp}"
                }).encode())

            elif topic == "cp.telemetry":
                sess = ACTIVE_SESSIONS.get(cp_id)
                if not sess:
                    continue

                # Notificamos el inicio de sesion una unica vez
                if not sess.get("notified_started"):
                    started_item = {
                        "ts": now_iso(),
                        "cp_id": cp_id,
                        "driver_id": sess["driver_id"],
                        "session_id": sess["session_id"],
                    }
                    await notify_panel({"type": "session.started", **started_item})
                    sess["notified_started"] = True

                kwh_total = data.get("kwh_total", 0.0)
                eur_total = data.get("eur_total", 0.0)

                # 1) Persistir progreso de la sesión
                update_session_progress(sess["session_id"], kwh_total, eur_total)
                log_event(cp_id, sess["driver_id"], "TELEMETRY", data)

                # 2) Guardar última telemetría en memoria (para API/panel)
                LAST_TELEMETRY[cp_id] = {
                    "kwh_total": kwh_total,
                    "eur_total": eur_total,
                    "driver_id": sess["driver_id"]
                }

                # 3) Asegurar que el CP queda en estado SUMINISTRANDO mientras llegan telemetrías
                update_cp(cp_id, "SUMINISTRANDO")

                # 4) Reenviar al Driver (app del conductor)
                await kafka_producer.send_and_wait("driver.telemetry", json.dumps({
                    "driver_id": sess["driver_id"],
                    "request_id": sess["request_id"],
                    "cp_id": cp_id,
                    "kw": data.get("kw"),
                    "kwh_total": kwh_total,
                    "eur_total": eur_total
                }).encode())

                # 5) Notificar al panel con status verde SUMINISTRANDO + totales + driver
                await notify_panel({
                    "type": "telemetry",
                    "cp_id": cp_id,
                    "driver_id": sess["driver_id"],
                    "kwh_total": kwh_total,
                    "eur_total": eur_total,
                    "status": "SUMINISTRANDO"
                })


            elif topic == "cp.session_ended":
                sess = ACTIVE_SESSIONS.pop(cp_id, None)
                reason = (data.get("reason") or "ENDED").upper()

                # Limpia última telemetría en memoria (ya terminó)
                LAST_TELEMETRY.pop(cp_id, None)

                # Si fue STOP/ABORTED, PARADO; si no, ACTIVADO
                new_status = "PARADO" if reason == "ABORTED" else "ACTIVADO"

                # Si hay alerta en la location del CP → queda PARADO sí o sí
                row = get_cp_from_db(cp_id) or {}
                loc_key = norm_loc(row.get("location"))
                if WEATHER_STATE.get(loc_key, {}).get("alert"):
                    new_status = "PARADO"
                    WEATHER_DISABLED_CPS.add(cp_id)

                update_cp(cp_id, new_status)
                await notify_panel({"type": "status", "cp_id": cp_id, "status": new_status})

                # Si estaba pendiente de parar por clima y sigue habiendo alerta → ahora sí mandamos STOP
                if cp_id in WEATHER_PENDING_STOP:
                    if WEATHER_STATE.get(loc_key, {}).get("alert"):
                        WEATHER_PENDING_STOP.discard(cp_id)

                        ok = await send_cp_command("STOP", cp_id, source="weather.after_session")
                        if not ok:
                            # Reintentar más tarde
                            WEATHER_PENDING_STOP.add(cp_id)

                            await notify_panel({
                                "type": "weather.command_failed",
                                "ts": now_iso(),
                                "cp_id": cp_id,
                                "action": "STOP",
                                "source": "weather.after_session"
                            })

                            log_central_msg("WEATHER_COMMAND_FAILED", {
                                "cp_id": cp_id,
                                "action": "STOP",
                                "source": "weather.after_session"
                            })
                        else:
                            log_central_msg("WEATHER_STOP_AFTER_SESSION", {
                                "cp_id": cp_id,
                                "location": row.get("location")
                            })
                    else:
                        # Se canceló la alerta antes de terminar
                        WEATHER_PENDING_STOP.discard(cp_id)



                if sess:
                    # Datos finales de la sesión
                    kwh_final = data.get("kwh")
                    amount_final = data.get("amount_eur")

                    # Saca location y precio del CP desde BD
                    cp_row = get_cp_from_db(cp_id) or {}
                    location = cp_row.get("location")
                    unit_price = cp_row.get("price_eur_kwh")

                    # Si no viene amount_eur, intenta calcularlo con el precio
                    if amount_final is None and kwh_final is not None and unit_price is not None:
                        try:
                            amount_final = round(float(kwh_final) * float(unit_price), 2)
                        except Exception:
                            pass

                    # Cierra sesión en BD
                    end_session(
                        sess["session_id"],
                        kwh_final,
                        amount_final,
                        "ENDED" if reason == "ENDED" else "ABORTED"
                    )

                    log_event(cp_id, sess["driver_id"], "END", {
                        "kwh": kwh_final,
                        "amount_eur": amount_final,
                        "reason": reason
                    })

                    # ENVÍA TICKET AL DRIVER (añadimos location y precio por kWh)
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": sess["driver_id"],
                        "request_id": sess["request_id"],
                        "status": "FINISHED",
                        "message": f"Servicio finalizado en {cp_id}",
                        "summary": {
                            "cp_id": cp_id,
                            "location": location,
                            "price_eur_kwh": unit_price,
                            "kwh": kwh_final,
                            "amount_eur": amount_final,
                            "reason": reason
                        }
                    }).encode())

                    log_central_msg("SUMINISTRO_FINALIZADO", {
                        "cp_id": cp_id, "reason": reason,
                        "driver_id": sess["driver_id"],
                        "kwh": kwh_final, "amount_eur": amount_final})

                    row = get_cp_from_db(cp_id) or {}
                    insert_audit_log(row.get("ip") or "unknown", "CP_E_"+cp_id, "SESSION_ENDED", f"Sesión {sess['session_id']} finalizada, kwh: {kwh_final}, eur: {amount_final}, reason: {reason}")
                    # borra del panel de sesiones iniciadas
                    await notify_panel({
                        "type": "session.ended",
                        "ts": now_iso(),
                        "cp_id": cp_id,
                        "driver_id": sess["driver_id"],
                        "session_id": sess["session_id"],
                        "kwh": kwh_final,
                        "amount_eur": amount_final,
                        "reason": reason
                    })
            elif topic == "engine.start_manual":
                acc_cp   = data.get("cp_id")
                acc_drv  = data.get("driver_id")
                acc_req  = data.get("request_id")

                # Validaciones básicas
                with closing(get_db()) as con:
                    row = db_get_cp(con, acc_cp)
                if not row:
                    # CP desconocido
                    log_central_msg("INICIO_MANUAL_DENEGADO", {"cp_id": acc_cp, "driver_id": acc_drv, "reason": "CP not found"})
                    continue
                if acc_cp in ACTIVE_SESSIONS:
                    # Ocupado: no crear sesión manual
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": acc_drv, "request_id": acc_req,
                        "status": "DENIED", "message": f"CP {acc_cp} ocupado"
                    }).encode())
                    log_central_msg("INICIO_MANUAL_DENEGADO", {"cp_id": acc_cp, "driver_id": acc_drv, "reason": "busy"})
                    continue
                if row["status"] not in ("ACTIVADO",):
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": acc_drv, "request_id": acc_req,
                        "status": "DENIED", "message": f"CP {acc_cp} no disponible (estado {row['status']})"
                    }).encode())
                    log_central_msg("INICIO_MANUAL_DENEGADO", {"cp_id": acc_cp, "driver_id": acc_drv, "reason": f"status={row['status']}"})
                    continue

                # Crear sesión y autorizar
                price = row["price_eur_kwh"]
                session_id = start_session(acc_cp, acc_drv, price)
                ACTIVE_SESSIONS[acc_cp] = {"driver_id": acc_drv, "request_id": acc_req, "session_id": session_id}

                #informa al driver, por si está conectado
                await kafka_producer.send_and_wait("driver.update", json.dumps({
                    "driver_id": acc_drv, "request_id": acc_req,
                    "status": "AUTHORIZED", "message": f"Autorizado (manual) en {acc_cp}"
                }).encode())

                # Autoriza al Engine
                await kafka_producer.send_and_wait("central.authorize", json.dumps({
                    "cp_id": acc_cp, "driver_id": acc_drv, "request_id": acc_req
                }).encode())

                # Panel: una sola vez aquí
                started_item = {"ts": now_iso(), "cp_id": acc_cp, "driver_id": acc_drv, "session_id": session_id}
                RECENT_SESSIONS.appendleft(started_item)
                log_central_msg("SUMINISTRO_MANUAL_INICIADO", {"cp_id": acc_cp, "driver_id": acc_drv, "session_id": session_id})

                row = get_cp_from_db(acc_cp)
                insert_audit_log(row.get("ip") or "unknown", "CP_E_"+acc_cp, "MANUAL_SESSION_STARTED", f"Sesión manual {session_id} iniciada en CP {acc_cp} para driver {acc_drv}")
            elif topic == "engine.reject":
                rej_cp   = data.get("cp_id")
                rej_drv  = data.get("driver_id")
                rej_req  = data.get("request_id")
                reason   = data.get("reason") or "REJECTED_BY_ENGINE"

                sess = ACTIVE_SESSIONS.get(rej_cp)
                if not sess:
                    # No hay sesión activa: solo loguea
                    log_central_msg("ENGINE_RECHAZA_SUMINISTRO", {"cp_id": rej_cp, "driver_id": rej_drv, "reason": "no active session"})
                    row = get_cp_from_db(rej_cp)
                    insert_audit_log(row.get("ip") or "unknown", "CP_E_"+rej_cp, "ENGINE_REJECT_NO_SESSION", f"Engine rechazó suministro en CP {rej_cp} pero no había sesión activa")
                else:
                    # comprobar coincidencia (si no coincide, también se limpia por seguridad)
                    if (sess.get("driver_id") == rej_drv) and (sess.get("request_id") == rej_req):
                        # Marcar sesión como REJECTED con 0 kWh / 0 €
                        end_session(sess["session_id"], 0.0, 0.0, ended_status="REJECTED")
                        # Notificar al driver
                        await kafka_producer.send_and_wait("driver.update", json.dumps({
                            "driver_id": rej_drv,
                            "request_id": rej_req,
                            "status": "DENIED",
                            "message": f"Suministro rechazado por CP {rej_cp}",
                            "summary": {
                                "cp_id": rej_cp,
                                "kwh": 0.0,
                                "amount_eur": 0.0,
                                "reason": "REJECTED"
                            }
                        }).encode())

                        # Actualiza estado del CP (vuelve a ACTIVADO si no estaba parado)
                        update_cp(rej_cp, "ACTIVADO")
                        await notify_panel({"type": "status", "cp_id": rej_cp, "status": "ACTIVADO"})

                        # Limpieza de buffers
                        ACTIVE_SESSIONS.pop(rej_cp, None)
                        LAST_TELEMETRY.pop(rej_cp, None)

                        # Panel: quitar de “Sesiones iniciadas”
                        await notify_panel({
                            "type": "session.ended",
                            "ts": now_iso(),
                            "cp_id": rej_cp,
                            "driver_id": rej_drv,
                            "session_id": sess["session_id"],
                            "kwh": 0.0,
                            "amount_eur": 0.0,
                            "reason": "REJECTED"
                        })

                        # Últimos 5 mensajes
                        log_central_msg("SUMINISTRO_RECHAZADO", {
                            "cp_id": rej_cp, "driver_id": rej_drv, "request_id": rej_req
                        })
                        row = get_cp_from_db(rej_cp)
                        insert_audit_log(row.get("ip") or "unknown", "CP_E_"+rej_cp, "ENGINE_REJECTED", f"Engine rechazó suministro en CP {rej_cp} para driver {rej_drv}, sesión {sess['session_id']}")
                    else:
                        ACTIVE_SESSIONS.pop(rej_cp, None)
                        update_cp(rej_cp, "ACTIVADO")
                        log_central_msg("ENGINE_REJECT_MISMATCH", {
                            "cp_id": rej_cp, "driver_id": rej_drv, "request_id": rej_req
                        })
                        row = get_cp_from_db(rej_cp)
                        insert_audit_log(row.get("ip") or "unknown", "CP_E_"+rej_cp, "ENGINE_REJECT_MISMATCH", f"Engine rechazó suministro en CP {rej_cp} pero driver/request no coinciden con sesión activa")



    finally:
        await kafka_consumer.stop()


async def produce_kafka():
    global kafka_producer
    kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await kafka_producer.start()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

async def main():
    global MAIN_LOOP
    init_db()
    init_central_auth_tables()
    asyncio.create_task(notify_central_state())

    # Al iniciar, marcar todos los CPs como DESCONECTADOS
    mark_all_cps_disconnected()


    # Guarda loop principal
    MAIN_LOOP = asyncio.get_running_loop()

    # Hilo servidor monitores
    threading.Thread(target=monitor_socket_server, args=(MAIN_LOOP,), daemon=True).start()

    # Kafka
    asyncio.create_task(produce_kafka())
    asyncio.create_task(consume_kafka())


    # Tarea de control de desconexiones de CP
    asyncio.create_task(monitor_disconnections())

    # HTTP + WS del panel
    # quevedo, he añadido el access_log=False para que no salga todo el rato en consola los accesos (los get)
    config = uvicorn.Config(app, host="0.0.0.0", port=HTTP_PORT, log_level="info", access_log=False)
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
