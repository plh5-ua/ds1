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
    schema = """
    CREATE TABLE IF NOT EXISTS charging_points (
        id TEXT PRIMARY KEY,
        location TEXT DEFAULT 'UNKNOWN',
        price_eur_kwh REAL DEFAULT 0.30,
        status TEXT DEFAULT 'DESCONECTADO',
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    """
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
                print(f"Updated{status}")
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
                update_cp(cp_id, "ACTIVADO")
                print(f"mi id: {cp_id}")
                await notify_panel({"type": "heartbeat", "cp_id": cp_id})
            elif topic == "driver.request":
                # data esperado: { "cp_id": "...", "driver_id": "...", "request_id": "..." }
                # Asegura que el producer está listo (arrancan en paralelo)
                while kafka_producer is None:
                    await asyncio.sleep(0.05)

                # 1) (opcional) validar CP en BD; por simplicidad, autorizamos siempre
                # 2) Avisar al Engine (si está ejecutándose) para que empiece a cargar

                req_cp = data["cp_id"]
                req_driver = data["driver_id"]
                req_id = data["request_id"]
                print(f"[driver.request] {data}")
                # Esto nos permitirá reenviar telemetrías y el FINISHED al driver correcto.
                ACTIVE_SESSIONS[req_cp] = {"driver_id": req_driver, "request_id": req_id}

                await kafka_producer.send_and_wait(
                    "central.authorize",
                    json.dumps({
                        "cp_id": req_cp,
                        "driver_id": req_driver,
                        "request_id": req_id
                    }).encode()
                )

                # 3) Notificar al Driver
                await kafka_producer.send_and_wait(
                    "driver.update",
                    json.dumps({
                        "driver_id": req_driver,
                        "request_id": req_id,
                        "status": "AUTHORIZED",
                        "message": f"Autorizado en {req_cp}"
                    }).encode()
                )

                print(f"[central.authorize] -> {data['cp_id']}  | [driver.update] AUTHORIZED")

            elif topic == "cp.telemetry":
                sess = ACTIVE_SESSIONS.get(cp_id)
                if not sess:
                    # No sabemos qué driver pidió esta sesión (no autorizada por CENTRAL)
                    continue
                payload = {
                    "driver_id": sess["driver_id"],
                    "request_id": sess["request_id"],
                    "cp_id": cp_id,
                    "kw": data.get("kw"),
                    "kwh_total": data.get("kwh_total"),
                    "eur_total": data.get("eur_total"),
                }
                await kafka_producer.send_and_wait("driver.telemetry", json.dumps(payload).encode())

            elif topic == "cp.session_ended":
                sess = ACTIVE_SESSIONS.pop(cp_id, None)  # liberamos el CP
                summary = {
                    "kwh": data.get("kwh"),
                    "amount_eur": data.get("amount_eur"),
                    "reason": data.get("reason", "ENDED"),
                }

                # (Opcional) actualizar DB de CP a ACTIVADO y notificar panel
                update_cp(cp_id, "ACTIVADO")
                await notify_panel({"type": "status", "cp_id": cp_id, "status": "ACTIVADO"})

                if sess:
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
