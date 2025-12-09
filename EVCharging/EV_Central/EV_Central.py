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

# --- FastAPI / WebSocket ---
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
import uvicorn
from fastapi.responses import HTMLResponse
from pathlib import Path
from pydantic import BaseModel




# --- Kafka asíncrono ---
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

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

            if action == "REGISTER":
                location = msg.get("location", "Desconocida")
                price    = float(msg.get("price", 0.30))
                insert_cp(cp_id, location, price)
                cp_data = get_cp_from_db(cp_id)
                print(f"🆕 CP registrado desde monitor: {cp_id} ({location}, {price} €/kWh)")
                try: conn.sendall(b"ACK")
                except Exception: pass
                # notificar panel desde el loop ASYNC
                asyncio.run_coroutine_threadsafe(
                    notify_panel({"type": "register", **cp_data}), loop
                )
                log_central_msg("REGISTRO_CP", {"cp_id": cp_id, "location": location, "price": price})

            elif action == "HEARTBEAT":
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
                        conn.sendall(health.encode())
                    except Exception:
                        pass
                    LAST_STATUS_SEEN[cp_id] = prev_status
                    return

                # Evitar pasar a Activaddo si esta parado esste if es probable
                if new_status == "ACTIVADO" and db_status == "PARADO":
                    try:
                        conn.sendall(health.encode())
                    except Exception:
                        pass
                    LAST_STATUS_SEEN[cp_id] = prev_status
                    return
                
                 # Evitar pasar a ACTIVADO si ya está suministrando
                if not (new_status == "ACTIVADO" and is_suministrando_cp(cp_id)):
                    # --- LOG de transición (una sola vez por cambio) ---
                    if prev_status != new_status:
                        if prev_status == "AVERIA" and new_status == "ACTIVADO":
                            log_central_msg("AVERIA SOLUCIONADA", {"cp_id": cp_id, "from": prev_status, "to": new_status})
                        elif prev_status == "ACTIVADO" and new_status == "AVERIA":
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
                    conn.sendall(health.encode())
                except Exception:
                    pass



            else:
                try: conn.sendall(b"NACK")
                except Exception: pass

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

                # Si estaba suministrando y el nuevo estado no permite suministro → cerrar
                #if status in {"AVERIA", "PARADO", "DESCONECTADO"} and cp_id in ACTIVE_SESSIONS:
                 #   reason_code = "FAULT" if status == "AVERIA" else ("DISCONNECTED" if status == "DESCONECTADO" else "ABORTED")
                  #  await force_close_session(cp_id, reason_code)

                update_cp(cp_id, status)
                await notify_panel({"type": "status", "cp_id": cp_id, "status": status})



            elif topic == "driver.request":
                req_cp = data["cp_id"]; req_driver = data["driver_id"]; req_id = data["request_id"]
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
                update_cp(cp_id, new_status)
                await notify_panel({"type": "status", "cp_id": cp_id, "status": new_status})

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
            elif topic == "engine.reject":
                rej_cp   = data.get("cp_id")
                rej_drv  = data.get("driver_id")
                rej_req  = data.get("request_id")
                reason   = data.get("reason") or "REJECTED_BY_ENGINE"

                sess = ACTIVE_SESSIONS.get(rej_cp)
                if not sess:
                    # No hay sesión activa: solo loguea
                    log_central_msg("ENGINE_RECHAZA_SUMINISTRO", {"cp_id": rej_cp, "driver_id": rej_drv, "reason": "no active session"})
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
                    else:
                        ACTIVE_SESSIONS.pop(rej_cp, None)
                        update_cp(rej_cp, "ACTIVADO")
                        log_central_msg("ENGINE_REJECT_MISMATCH", {
                            "cp_id": rej_cp, "driver_id": rej_drv, "request_id": rej_req
                        })



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
    config = uvicorn.Config(app, host="0.0.0.0", port=HTTP_PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
