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
HEARTBEAT_TIMEOUT = 3  # segundos sin recibir heartbeat → DESCONECTADO


# ---------------------------------------------------------------------------
# ARGUMENTOS DE EJECUCIÓN
# ---------------------------------------------------------------------------

if len(sys.argv) < 2:
    print("Uso: python EV_Central.py <puerto_http> <ip_broker:puerto>")
    sys.exit(1)

HTTP_PORT = int(sys.argv[1])
KAFKA_BOOTSTRAP = sys.argv[2] if len(sys.argv) > 2 else "127.0.0.1:9092"
SOCKET_PORT = 9000  # puerto para monitores

print("🔌 Iniciando EV_Central ...")
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
    print("🔘 Todos los puntos de recarga marcados como DESCONECTADOS al iniciar CENTRAL.")

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
    return list_cps()


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    PANEL_CLIENTS.add(ws)
    print("📡 Cliente conectado al panel")
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        PANEL_CLIENTS.discard(ws)
        print("🔌 Cliente desconectado")

@app.on_event("shutdown")
async def shutdown_event():
    try:
        print("🧹 Cerrando conexión Kafka...")
        await kafka_consumer.stop()
        await kafka_producer.stop()
        print("✅ Kafka cerrado correctamente.")
    except Exception as e:
        print("⚠️ Error cerrando Kafka:", e)


async def notify_panel(event: Dict[str, Any]):
    dead = []
    for ws in list(PANEL_CLIENTS):
        try:
            await ws.send_text(json.dumps(event))
        except Exception:
            dead.append(ws)
    for ws in dead:
        PANEL_CLIENTS.discard(ws)

class Command(BaseModel):
    action: str           # "STOP" | "RESUME"
    cp_id: str = "ALL"    # puedes enviar "ALL" o un ID concreto

@app.post("/command")
async def send_command(cmd: Command):
    payload = {"action": cmd.action.upper(), "cp_id": cmd.cp_id}
    await kafka_producer.send_and_wait("central.command", json.dumps(payload).encode())
    return {"status": "ok", "sent": payload}



# ---------------------------------------------------------------------------
# SOCKET SERVER (para MONITORES)
# ---------------------------------------------------------------------------

def monitor_socket_server(loop):
    """
    Recibe registros y heartbeats desde los monitores vía socket (bloqueante en hilo).
    Protocolo:
       - REGISTER: { "action": "REGISTER", "cp_id": "...", "location": "...", "price": 0.58 }
         → inserta/actualiza en BD, notifica panel
       - HEARTBEAT: { "action": "HEARTBEAT", "cp_id": "...", "health": "OK"/"KO" }
         → actualiza estado a ACTIVADO/AVERIA, notifica panel
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("0.0.0.0", SOCKET_PORT))
    srv.listen(8)
    print(f"🧭 Escuchando monitores en puerto {SOCKET_PORT}...")

    while True:
        conn, _addr = srv.accept()
        try:
            data = conn.recv(4096)
            if not data:
                conn.close()
                continue

            try:
                msg = json.loads(data.decode())
            except Exception:
                # compat: si el monitor envía texto plano “PING”
                if data == b"PING":
                    conn.sendall(b"OK")
                else:
                    conn.sendall(b"NACK")
                conn.close()
                continue

            action = (msg.get("action") or "").upper()
            cp_id = msg.get("cp_id")

            if action == "REGISTER":
                location = msg.get("location", "Desconocida")
                price = float(msg.get("price", 0.30))
                insert_cp(cp_id, location, price)
                cp_data = get_cp_from_db(cp_id)
                print(f"🆕 CP registrado desde monitor: {cp_id} ({location}, {price} €/kWh)")
                # ACK al monitor si lo espera
                try:
                    conn.sendall(b"ACK")
                except Exception:
                    pass
                asyncio.run_coroutine_threadsafe(
                    notify_panel({"type": "register", **cp_data}), loop,
                )

            elif action == "HEARTBEAT":
                # registrar la hora del último heartbeat recibido
                LAST_HEARTBEAT[cp_id] = loop.time()

                health = (msg.get("health") or "KO").upper()
                new_status = "ACTIVADO" if health == "OK" else "AVERIA"
                if new_status == "ACTIVADO" and is_suministrando_cp(cp_id):
                    pass
                else:
                    update_cp(cp_id, new_status)
                    cp_data = get_cp_from_db(cp_id) or {"id": cp_id, "status": new_status}
                    cp_data["status"] = new_status
                    asyncio.run_coroutine_threadsafe(
                        notify_panel({"type": "heartbeat", **cp_data}), loop,
                )
                try:
                    conn.sendall(health.encode())
                except Exception:
                    pass

            else:
                try:
                    conn.sendall(b"NACK")
                except Exception:
                    pass

        except Exception as e:
            print(f"⚠️ Error procesando monitor: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

async def monitor_disconnections():
    """🧠 Marca CPs como DESCONECTADOS si no envían heartbeats recientes."""
    while True:
        now = asyncio.get_running_loop().time()
        for cp_id, last in list(LAST_HEARTBEAT.items()):
            if now - last > HEARTBEAT_TIMEOUT:
                print(f"⚠️ CP {cp_id} no ha mandado heartbeat en {HEARTBEAT_TIMEOUT}s → DESCONECTADO")
                update_cp(cp_id, "DESCONECTADO")
                cp_data = get_cp_from_db(cp_id)
                if cp_data:
                    cp_data["status"] = "DESCONECTADO"
                    await notify_panel({"type": "heartbeat", **cp_data})
                # eliminar para no repetir
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


async def consume_kafka():
    global kafka_consumer
    kafka_consumer = AIOKafkaConsumer(
        "cp.heartbeat",
        "cp.status",
        "cp.register",
        "cp.telemetry",
        "cp.session_ended",
        "driver.request",
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

            if topic == "cp.register":
                # Registro de Engine (vía Kafka) — redundante con monitor, pero compatible
                insert_cp(cp_id, data.get("location", "Desconocida"), float(data.get("kwh", 0.30)))
                await notify_panel({"type": "status", "cp_id": cp_id, "status": data.get("status", "ACTIVADO")})

            elif topic == "cp.status":
                update_cp(cp_id, data.get("status", "ACTIVADO"))
                await notify_panel({"type": "status", "cp_id": cp_id, "status": data.get("status", "ACTIVADO")})

            elif topic == "cp.heartbeat":
                # Si un Engine publica health por Kafka (además del monitor)
                health = (data.get("health") or "OK").upper()
                new_status = "ACTIVADO" if health == "OK" else "AVERIA"
                update_cp(cp_id, new_status)
                await notify_panel({"type": "status", "cp_id": cp_id, "status": new_status})
                log_event(cp_id, None, "HEARTBEAT", {"health": health})

            elif topic == "driver.request":
                req_cp = data["cp_id"]; req_driver = data["driver_id"]; req_id = data["request_id"]
                print(f"[driver.request] {data}")

                # 1) ¿Ocupado?
                if req_cp in ACTIVE_SESSIONS:
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": req_driver, "request_id": req_id,
                        "status": "DENIED", "message": f"CP {req_cp} ocupado: hay una sesión en curso"
                    }).encode())
                    continue

                # 2) ¿Existe y estado?
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
                    "session_id": session_id
                }
                log_event(req_cp, req_driver, "AUTH", {"request_id": req_id})

                # 4) Avisar a Engine y Driver
                await kafka_producer.send_and_wait("central.authorize", json.dumps({
                    "cp_id": req_cp, "driver_id": req_driver, "request_id": req_id
                }).encode())
                await kafka_producer.send_and_wait("driver.update", json.dumps({
                    "driver_id": req_driver, "request_id": req_id,
                    "status": "AUTHORIZED", "message": f"Autorizado en {req_cp}"
                }).encode())
                print(f"[central.authorize] -> {req_cp}  | [driver.update] AUTHORIZED")

            elif topic == "cp.telemetry":
                sess = ACTIVE_SESSIONS.get(cp_id)
                if not sess:
                    # Telemetría de una sesión que CENTRAL no abrió
                    continue
                update_session_progress(sess["session_id"], data.get("kwh_total", 0.0), data.get("eur_total", 0.0))
                log_event(cp_id, sess["driver_id"], "TELEMETRY", data)
                # reenviar al Driver dueño de la request
                await kafka_producer.send_and_wait("driver.telemetry", json.dumps({
                    "driver_id": sess["driver_id"],
                    "request_id": sess["request_id"],
                    "cp_id": cp_id,
                    "kw": data.get("kw"),
                    "kwh_total": data.get("kwh_total"),
                    "eur_total": data.get("eur_total")
                }).encode())

                await notify_panel({
                    "type": "telemetry",
                    "cp_id": data["cp_id"],
                    "driver_id": data.get("driver_id"),
                    "kw": data.get("kw"),
                    "kwh_total": data.get("kwh_total"),
                    "eur_total": data.get("eur_total"),
                    "status": "SUMINISTRANDO"
                })

            elif topic == "cp.session_ended":
                sess = ACTIVE_SESSIONS.pop(cp_id, None)
                summary = {
                    "kwh": data.get("kwh"),
                    "amount_eur": data.get("amount_eur"),
                    "reason": data.get("reason", "ENDED")
                }

                # Liberar CP y notificar panel
                update_cp(cp_id, "ACTIVADO")
                await notify_panel({"type": "status", "cp_id": cp_id, "status": "ACTIVADO"})

                if sess:
                    end_session(sess["session_id"], summary["kwh"], summary["amount_eur"],
                                "ENDED" if summary["reason"] == "ENDED" else "ABORTED")
                    log_event(cp_id, sess["driver_id"], "END", summary)
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": sess["driver_id"],
                        "request_id": sess["request_id"],
                        "status": "FINISHED",
                        "message": f"Servicio finalizado en {cp_id}",
                        "summary": summary
                    }).encode())

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
    init_db()

    # Al iniciar, marcar todos los CPs como DESCONECTADOS
    mark_all_cps_disconnected()

    # Hilo para servidor de monitores (socket bloqueante)
    loop = asyncio.get_running_loop()
    threading.Thread(target=monitor_socket_server, args=(loop,), daemon=True).start()

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
