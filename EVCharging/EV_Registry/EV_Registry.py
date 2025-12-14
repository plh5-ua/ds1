# EV_Registry.py
import os
import base64
import hashlib
import hmac
import secrets
import sqlite3
from contextlib import closing
from datetime import datetime
from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import uvicorn

DB_PATH = os.getenv("EV_DB_PATH", r"..\EV_Central\evcentral.db")
REGISTRY_PORT = int(os.getenv("EV_REGISTRY_PORT", "7070"))

# IMPORTANTE: Central y Registry deben compartir el mismo PEPPER
PEPPER = os.getenv("EV_REGISTRY_PEPPER", "CHANGE_ME")

app = FastAPI(title="EV_Registry", version="1.0")


def get_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con


def init_registry_tables():
    with closing(get_db()) as con:
        con.executescript("""
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS cp_registry_credentials (
            cp_id TEXT PRIMARY KEY,
            cred_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            issued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            revoked INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_cp_registry_revoked
        ON cp_registry_credentials(revoked);
        """)
        con.commit()


def hash_cred(cred_plain: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        (cred_plain + PEPPER).encode("utf-8"),
        salt.encode("utf-8"),
        200_000,
    )
    return base64.urlsafe_b64encode(dk).decode().rstrip("=")


def issue_credential(cp_id: str) -> str:
    cred_plain = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
    salt = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("=")
    cred_hash = hash_cred(cred_plain, salt)

    with closing(get_db()) as con:
        con.execute("""
            INSERT INTO cp_registry_credentials(cp_id, cred_hash, salt, revoked)
            VALUES (?,?,?,0)
            ON CONFLICT(cp_id) DO UPDATE SET
                cred_hash=excluded.cred_hash,
                salt=excluded.salt,
                issued_at=CURRENT_TIMESTAMP,
                revoked=0
        """, (cp_id, cred_hash, salt))
        con.commit()

    return cred_plain


def revoke_credential(cp_id: str):
    with closing(get_db()) as con:
        con.execute("UPDATE cp_registry_credentials SET revoked=1 WHERE cp_id=?", (cp_id,))
        con.commit()


def upsert_cp(cp_id: str, location: str, price: float):
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
                "INSERT INTO charging_points(id, location, price_eur_kwh, status) VALUES (?,?,?, 'DESCONECTADO')",
                (cp_id, location, price),
            )
        con.commit()


def mark_cp_disconnected(cp_id: str):
    with closing(get_db()) as con:
        con.execute(
            "UPDATE charging_points SET status='DESCONECTADO', updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (cp_id,)
        )
        con.commit()


def get_cp(cp_id: str) -> Dict[str, Any]:
    with closing(get_db()) as con:
        row = con.execute("SELECT * FROM charging_points WHERE id=?", (cp_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="CP no encontrado")
        return dict(row)


class AltaReq(BaseModel):
    cp_id: str = Field(..., min_length=1)
    location: str = Field(..., min_length=1)
    price: float = Field(0.30, ge=0.0)


@app.on_event("startup")
def startup():
    init_registry_tables()


@app.get("/health")
def health():
    return {"ok": True, "service": "EV_Registry", "ts": datetime.now().isoformat(timespec="seconds")}


@app.put("/cp/{cp_id}")
def alta_cp(cp_id: str, payload: AltaReq):
    cp_id = cp_id.strip()
    if payload.cp_id.strip() != cp_id:
        raise HTTPException(status_code=400, detail="cp_id del path y body no coinciden")

    upsert_cp(cp_id, payload.location.strip(), float(payload.price))
    cred = issue_credential(cp_id)

    return {
        "ok": True,
        "cp": get_cp(cp_id),
        "credential": cred
    }


@app.delete("/cp/{cp_id}")
def baja_cp(cp_id: str):
    cp_id = cp_id.strip()
    # si no existe, 404
    _ = get_cp(cp_id)
    revoke_credential(cp_id)
    mark_cp_disconnected(cp_id)
    return {"ok": True, "cp_id": cp_id, "revoked": True, "status": "DESCONECTADO"}


@app.get("/cp/{cp_id}")
def get_cp_info(cp_id: str):
    return {"ok": True, "cp": get_cp(cp_id)}


# uvicorn EV_Registry:app --host 0.0.0.0 --port 7070 --ssl-keyfile key.pem --ssl-certfile cert.pem
if __name__ == "__main__":
    uvicorn.run(
        "EV_Registry:app",
        host="0.0.0.0",
        port=8443,
        ssl_certfile="cert.pem",
        ssl_keyfile="key.pem"
    )

