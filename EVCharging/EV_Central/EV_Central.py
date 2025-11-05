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

# --- Kafka asíncrono ---
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer


# ---------------------------------------------------------------------------
# ARGUMENTOS DE EJECUCIÓN
# ---------------------------------------------------------------------------

if len(sys.argv) < 3:
    print("Uso: python EV_Central.py <puerto_http> <ip_broker:puerto>")
    sys.exit(1)

HTTP_PORT = int(sys.argv[1])
KAFKA_BOOTSTRAP = sys.argv[2]
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


def insert_cp(cp_id: str, location: str, price: float = 0.3):
    with closing(get_db()) as con:
        cur = con.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO charging_points(id, location, price_eur_kwh) VALUES (?, ?, ?)",
            (cp_id, location, price),
        )
        con.commit()


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


# ---------------------------------------------------------------------------
# FASTAPI + PANEL
# ---------------------------------------------------------------------------

app = FastAPI(title="EV_Central")
app.mount("/static", StaticFiles(directory="static"), name="static")

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
# SOCKET SERVER (para MONITORES)
# ---------------------------------------------------------------------------

def monitor_socket_server(loop):
    """Recibe registros y heartbeats desde los monitores vía socket."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("0.0.0.0", SOCKET_PORT))
    srv.listen(5)
    print(f"🧭 Escuchando monitores en puerto {SOCKET_PORT}...")

    while True:
        conn, addr = srv.accept()
        data = conn.recv(1024)
        if not data:
            conn.close()
            continue
        try:
            msg = json.loads(data.decode())
            action = msg.get("action")
            cp_id = msg.get("cp_id")
            location = msg.get("location", "Desconocida")

            if action == "REGISTER":
                insert_cp(cp_id, location)
                print(f"🆕 CP registrado desde monitor: {cp_id} ({location})")
                asyncio.run_coroutine_threadsafe(
                    notify_panel(
                        {"type": "register", "cp_id": cp_id, "status": "ACTIVADO"}
                    ),
                    loop,
                )

            elif action == "HEARTBEAT":
                health = msg.get("health", "KO")
                new_status = "ACTIVADO" if health == "OK" else "AVERÍA"
                update_cp(cp_id, new_status)
                #print(f"❤️‍🔥 Heartbeat {cp_id}: {new_status}")
                asyncio.run_coroutine_threadsafe(
                    notify_panel(
                        {"type": "heartbeat", "cp_id": cp_id, "status": new_status}
                    ),
                    loop,
                )

        except Exception as e:
            print(f"⚠️ Error procesando mensaje: {e}")
        conn.close()


# ---------------------------------------------------------------------------
# KAFKA (solo para Engines y Drivers)
# ---------------------------------------------------------------------------

kafka_producer = None


async def consume_kafka():
    consumer = AIOKafkaConsumer(
        "driver.request",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    )
    await consumer.start()

    global kafka_producer
    while kafka_producer is None:
        await asyncio.sleep(0.1)

    try:
        async for msg in consumer:
            data = msg.value
            cp_id = data.get("cp_id")
            driver_id = data.get("driver_id")
            request_id = data.get("request_id")

            # Enviar autorización al Engine correspondiente
            await kafka_producer.send_and_wait(
                "central.authorize",
                json.dumps(
                    {"cp_id": cp_id, "driver_id": driver_id, "request_id": request_id}
                ).encode(),
            )
            # Notificar al Driver
            await kafka_producer.send_and_wait(
                "driver.update",
                json.dumps(
                    {
                        "driver_id": driver_id,
                        "request_id": request_id,
                        "status": "AUTHORIZED",
                        "message": f"Autorizado en {cp_id}",
                    }
                ).encode(),
            )
            print(f"🚗 Petición de {driver_id} -> {cp_id} autorizada.")
    finally:
        await consumer.stop()


async def produce_kafka():
    global kafka_producer
    kafka_producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    await kafka_producer.start()


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

async def main():
    init_db()

    loop = asyncio.get_running_loop()
    threading.Thread(target=monitor_socket_server, args=(loop,), daemon=True).start()

    asyncio.create_task(produce_kafka())
    asyncio.create_task(consume_kafka())

    config = uvicorn.Config(app, host="0.0.0.0", port=HTTP_PORT, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()


if __name__ == "__main__":
    asyncio.run(main())
