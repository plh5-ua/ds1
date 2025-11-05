"""
EV_Central — Sistema central de monitorización de puntos de recarga
Uso:
    python central.py <puerto_http> <ip_broker:puerto> [<ip_db:puerto>]

Ejemplo:
    python central.py 8080 127.0.0.1:9092 127.0.0.1:0
"""

import sys
import os
import json
import asyncio
import sqlite3
from contextlib import closing
from typing import Dict, Any

# --- FastAPI / WebSocket ---
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

# --- Kafka asíncrono ---
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


# ---------------------------------------------------------------------------
# ARGUMENTOS DE EJECUCIÓN
# ---------------------------------------------------------------------------

if len(sys.argv) < 3:
    print("Uso: python central.py <puerto_http> <ip_broker:puerto> [<ip_db:puerto>]")
    sys.exit(1)

HTTP_PORT = int(sys.argv[1])
KAFKA_BOOTSTRAP = sys.argv[2]
DB_ADDR = sys.argv[3] if len(sys.argv) > 3 else "127.0.0.1:0"  # no se usa en SQLite

print("🔌 Iniciando EV_Central ...")
print(f"  • Puerto HTTP: {HTTP_PORT}")
print(f"  • Broker Kafka: {KAFKA_BOOTSTRAP}")
print(f"  • Dirección BBDD: {DB_ADDR}")

DB_PATH = "evcentral.db"

# ---------------------------------------------------------------------------
# BASE DE DATOS SQLITE
# ---------------------------------------------------------------------------

def get_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    with open("schema.sql", "r", encoding="utf-8") as f:
        schema = f.read()
    with closing(get_db()) as con:
        con.executescript(schema)
        con.commit()



def update_cp(cp_id: str, status: str = None):
    with closing(get_db()) as con:
        cur = con.cursor()
        row = cur.execute("SELECT id FROM charging_points WHERE id=?", (cp_id,)).fetchone()
        if row:
            if status:
                cur.execute(
                    "UPDATE charging_points SET status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                    (status, cp_id)
                )
                print(f"Updated {status}")
        else:
            print(f"ERROR: no hay un CP con ese id registrado, no se actualiza la base de datos.")

        con.commit()

def insert_cp(cp_id: str, cp_location: str, kwh: float, status: str = None):
    with closing(get_db()) as con:
        cur = con.cursor()
        row = cur.execute("SELECT id FROM charging_points WHERE id=?", (cp_id,)).fetchone()
        if row:
            print(f"ERROR: no se puede crear dos puntos de carga con el mismo id")
        else:
            cur.execute(
                "INSERT INTO charging_points(id, location, price_eur_kwh) VALUES(?, ?, ?)",
                (cp_id, cp_location, kwh),
            )
        con.commit()

def list_cps():
    with closing(get_db()) as con:
        rows = con.execute("SELECT * FROM charging_points").fetchall()
        return [dict(r) for r in rows]


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

class DriverRequest(BaseModel):
    cp_id: str
    driver_id: str
    request_id: str


@app.get("/cp")
def api_list_cps():
    return list_cps()


# --- WebSocket para el panel ---
PANEL_CLIENTS = set()

@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    PANEL_CLIENTS.add(ws)
    print("📡 Cliente conectado al panel")
    try:
        while True:
            await ws.receive_text()  # solo mantener viva la conexión
    except WebSocketDisconnect:
        PANEL_CLIENTS.discard(ws)
        print("🔌 Cliente desconectado")


async def notify_panel(event: Dict[str, Any]):
    dead = []
    for ws in list(PANEL_CLIENTS):
        try:
            await ws.send_text(json.dumps(event))
        except Exception:
            dead.append(ws)
    for ws in dead:
        PANEL_CLIENTS.discard(ws)


# ---------------------------------------------------------------------------
# KAFKA (PRODUCER + CONSUMER)
# ---------------------------------------------------------------------------

kafka_consumer = None
kafka_producer = None
# Sesiones activas por CP (mapeo en memoria)
ACTIVE_SESSIONS = {}  # cp_id -> {"driver_id":..., "request_id":...}


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
    )

    await kafka_consumer.start()
    try:
        async for msg in kafka_consumer:
            topic = msg.topic
            data = msg.value
            cp_id = data.get("cp_id")
            cp_location = data.get("location")
            kwh = data.get("kwh")
            if topic == "cp.register":
                print(f"Iniciando registro de CP {cp_id}")
                insert_cp(cp_id, cp_location, kwh)
                await notify_panel({"type": "status", "cp_id": cp_id, "status": data.get("status")})
            elif topic == "cp.status":
                update_cp(cp_id, data.get("status"))
                await notify_panel({"type": "status", "cp_id": cp_id, "status": data.get("status")})
            elif topic == "cp.heartbeat":
                health = data.get("health")
                new_status = "ACTIVADO" if health == "OK" else "AVERIA"
                update_cp(cp_id, new_status)
                await notify_panel({"type": "status", "cp_id": cp_id, "status": new_status})
                log_event(cp_id, None, "HEARTBEAT", {"health": health})
            elif topic == "driver.request":
                while kafka_producer is None:
                    await asyncio.sleep(0.05)

                req_cp = data["cp_id"]; req_driver = data["driver_id"]; req_id = data["request_id"]
                print(f"[driver.request] {data}")

                # ocupado
                if req_cp in ACTIVE_SESSIONS:
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": req_driver, "request_id": req_id,
                        "status": "DENIED", "message": f"CP {req_cp} ocupado: hay una sesión en curso"
                    }).encode())
                    continue

                # validar en BD
                with closing(get_db()) as con:
                    row = db_get_cp(con, req_cp)
                if not row:
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": req_driver, "request_id": req_id,
                        "status": "DENIED", "message": f"CP {req_cp} no registrado en CENTRAL"
                    }).encode()); continue

                cp_status = row["status"]
                if cp_status in ("DESCONECTADO", "AVERIA", "PARADO") or cp_status != "ACTIVADO":
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": req_driver, "request_id": req_id,
                        "status": "DENIED", "message": f"CP {req_cp} no disponible (estado {cp_status})"
                    }).encode()); continue

                # sesión RUNNING
                price = row["price_eur_kwh"]
                session_id = start_session(req_cp, req_driver, price)
                ACTIVE_SESSIONS[req_cp] = {"driver_id": req_driver, "request_id": req_id, "session_id": session_id}
                log_event(req_cp, req_driver, "AUTH", {"request_id": req_id})

                # avisar al engine + driver
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
                    continue
                update_session_progress(sess["session_id"], data.get("kwh_total", 0), data.get("eur_total", 0))
                log_event(cp_id, sess["driver_id"], "TELEMETRY", data)
                await kafka_producer.send_and_wait("driver.telemetry", json.dumps({
                    "driver_id": sess["driver_id"], "request_id": sess["request_id"], "cp_id": cp_id,
                    "kw": data.get("kw"), "kwh_total": data.get("kwh_total"), "eur_total": data.get("eur_total")
                }).encode())

            elif topic == "cp.session_ended":
                sess = ACTIVE_SESSIONS.pop(cp_id, None)
                summary = {"kwh": data.get("kwh"), "amount_eur": data.get("amount_eur"), "reason": data.get("reason", "ENDED")}
                update_cp(cp_id, "ACTIVADO")
                await notify_panel({"type": "status", "cp_id": cp_id, "status": "ACTIVADO"})
                if sess:
                    end_session(sess["session_id"], summary["kwh"], summary["amount_eur"],
                                "ENDED" if summary["reason"] == "ENDED" else "ABORTED")
                    log_event(cp_id, sess["driver_id"], "END", summary)
                    await kafka_producer.send_and_wait("driver.update", json.dumps({
                        "driver_id": sess["driver_id"], "request_id": sess["request_id"],
                        "status": "FINISHED", "message": f"Servicio finalizado en {cp_id}", "summary": summary
                    }).encode())





    finally:
        await kafka_consumer.stop()


async def produce_kafka():
    global kafka_producer
    kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await kafka_producer.start()


# ---------------------------------------------------------------------------
# ARRANQUE PRINCIPAL
# ---------------------------------------------------------------------------

async def main():
    init_db()
    # lanzar tareas Kafka
    asyncio.create_task(consume_kafka())
    asyncio.create_task(produce_kafka())

    # lanzar servidor HTTP FastAPI
    config = uvicorn.Config(app, host="0.0.0.0", port=HTTP_PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
