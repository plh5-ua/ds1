"""
EV_Registry — Registro REST de Charging Points (CPs)
- Alta / Baja / Consulta de CPs
- Emite credenciales para que el CP luego se autentique en EV_Central

Encaja con:
- Requisito: EV_Registry se comunica con EV_CP_M vía API REST y canal seguro :contentReference[oaicite:4]{index=4}
- BD compartida con EV_Central (tabla charging_points) 
"""

import os
import hmac
import json
import base64
import sqlite3
import secrets
import hashlib
from contextlib import closing
from datetime import datetime
from typing import Optional, Dict, Any

from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel, Field
import uvicorn


# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

DB_PATH = os.getenv("EV_DB_PATH", "evcentral.db")  # misma BD que usa EV_Central :contentReference[oaicite:6]{index=6}

# “Pepper” global del servidor (NO lo publiques). En producción vendría de ENV/secret manager.
PEPPER = os.getenv("EV_REGISTRY_PEPPER", "CHANGE_ME_IN_PROD")

app = FastAPI(title="EV_Registry", version="1.0")


# -------------------------------------------------------------------
# DB helpers
# -------------------------------------------------------------------

def get_db():
    con = sqlite3.connect(DB_PATH, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def init_registry_tables():
    """
    Crea tablas extra para credenciales del Registry.
    NO toca schema.sql existente, añade lo necesario para Registry.
    """
    with closing(get_db()) as con:
        con.executescript("""
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS cp_credentials (
            cp_id TEXT PRIMARY KEY,
            token_hash TEXT NOT NULL,
            salt TEXT NOT NULL,
            issued_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
            revoked INTEGER NOT NULL DEFAULT 0
        );

        CREATE INDEX IF NOT EXISTS idx_cp_credentials_revoked ON cp_credentials(revoked);
        """)
        con.commit()

def upsert_cp_in_charging_points(cp_id: str, location: str, price: float):
    """
    Inserta o actualiza el CP en charging_points (tabla que ya usa EV_Central) 
    """
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

def mark_cp_unregistered(cp_id: str):
    """
    Baja “lógica”: deja el CP en BD pero lo marca como DESCONECTADO.
    (Alternativa: borrarlo. Pero así Central conserva histórico/sesiones.)
    """
    with closing(get_db()) as con:
        cur = con.cursor()
        row = cur.execute("SELECT id FROM charging_points WHERE id=?", (cp_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="CP no existe en charging_points")
        cur.execute(
            "UPDATE charging_points SET status='DESCONECTADO', updated_at=CURRENT_TIMESTAMP WHERE id=?",
            (cp_id,)
        )
        con.commit()

def issue_token(cp_id: str) -> str:
    """
    Genera un token “secreto” que se entrega al CP (Monitor).
    Guardamos SOLO hash+salt en BD (nunca el token en claro).
    """
    token_plain = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
    salt = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("=")
    token_hash = hash_token(token_plain, salt)

    with closing(get_db()) as con:
        con.execute("""
            INSERT INTO cp_credentials(cp_id, token_hash, salt, revoked)
            VALUES (?,?,?,0)
            ON CONFLICT(cp_id) DO UPDATE SET
                token_hash=excluded.token_hash,
                salt=excluded.salt,
                issued_at=CURRENT_TIMESTAMP,
                revoked=0
        """, (cp_id, token_hash, salt))
        con.commit()

    return token_plain

def revoke_token(cp_id: str):
    with closing(get_db()) as con:
        cur = con.cursor()
        row = cur.execute("SELECT cp_id FROM cp_credentials WHERE cp_id=?", (cp_id,)).fetchone()
        if not row:
            return
        cur.execute("UPDATE cp_credentials SET revoked=1 WHERE cp_id=?", (cp_id,))
        con.commit()

def hash_token(token_plain: str, salt: str) -> str:
    """
    Hash fuerte y simple: PBKDF2-HMAC-SHA256(token + PEPPER, salt)
    """
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        (token_plain + PEPPER).encode("utf-8"),
        salt.encode("utf-8"),
        200_000
    )
    return base64.urlsafe_b64encode(dk).decode().rstrip("=")

def verify_token(cp_id: str, token_plain: str) -> bool:
    """
    Verifica un token presentado por el CP contra el hash guardado.
    Esto lo usaría EV_Central en un flujo de autenticación (socket o REST),
    o se puede exponer aquí como endpoint de “auth”.
    """
    with closing(get_db()) as con:
        row = con.execute("SELECT token_hash, salt, revoked FROM cp_credentials WHERE cp_id=?", (cp_id,)).fetchone()
        if not row or int(row["revoked"]) == 1:
            return False
        expected = row["token_hash"]
        salt = row["salt"]
        got = hash_token(token_plain, salt)
        return hmac.compare_digest(expected, got)

def get_cp(cp_id: str) -> Dict[str, Any]:
    with closing(get_db()) as con:
        row = con.execute("SELECT * FROM charging_points WHERE id=?", (cp_id,)).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="CP no encontrado")
        return dict(row)


# -------------------------------------------------------------------
# API models (request/response)
# -------------------------------------------------------------------

class RegisterReq(BaseModel):
    cp_id: str = Field(..., min_length=1)
    location: str = Field(..., min_length=1)
    price: float = Field(0.30, ge=0.0)

class RegisterResp(BaseModel):
    ok: bool
    cp: Dict[str, Any]
    credentials: Dict[str, Any]

class AuthReq(BaseModel):
    cp_id: str
    token: str

class AuthResp(BaseModel):
    ok: bool
    reason: Optional[str] = None


# -------------------------------------------------------------------
# API endpoints
# -------------------------------------------------------------------

@app.on_event("startup")
def on_startup():
    # Asegura tablas de credenciales
    init_registry_tables()

@app.get("/health")
def health():
    return {"ok": True, "service": "EV_Registry", "ts": datetime.now().isoformat(timespec="seconds")}

@app.post("/cp/register", response_model=RegisterResp)
def register_cp(payload: RegisterReq):
    """
    ALTA de CP:
    1) Upsert en charging_points (BD compartida con Central) 
    2) Emite credencial (token) y la devuelve al CP :contentReference[oaicite:9]{index=9}
    """
    cp_id = payload.cp_id.strip()
    location = payload.location.strip()

    upsert_cp_in_charging_points(cp_id, location, float(payload.price))
    token = issue_token(cp_id)

    cp = get_cp(cp_id)
    return {
        "ok": True,
        "cp": cp,
        "credentials": {
            "type": "token",
            "token": token,
            "note": "Guarda este token; no se volverá a mostrar igual."
        }
    }

@app.delete("/cp/{cp_id}")
def unregister_cp(cp_id: str):
    """
    BAJA de CP:
    - Marca el CP como DESCONECTADO
    - Revoca credenciales
    """
    cp_id = cp_id.strip()
    mark_cp_unregistered(cp_id)
    revoke_token(cp_id)
    return {"ok": True, "cp_id": cp_id, "status": "DESCONECTADO", "credentials_revoked": True}

@app.get("/cp/{cp_id}")
def get_cp_info(cp_id: str):
    """
    Consulta de CP (debug / panel / pruebas).
    """
    return {"ok": True, "cp": get_cp(cp_id)}

@app.post("/cp/auth", response_model=AuthResp)
def auth_cp(payload: AuthReq):
    """
    Endpoint de autenticación (opcional, pero útil):
    - Devuelve OK si el CP está registrado y el token coincide.
    """
    cp_id = payload.cp_id.strip()
    token = payload.token.strip()
    # Asegura que existe en charging_points (registrado)
    _ = get_cp(cp_id)

    if verify_token(cp_id, token):
        return {"ok": True}
    return {"ok": False, "reason": "INVALID_OR_REVOKED_TOKEN"}


# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------

def main():
    """
    Para canal seguro (HTTPS), lanza con:
      uvicorn EV_Registry:app --host 0.0.0.0 --port 7070 --ssl-keyfile key.pem --ssl-certfile cert.pem

    Para pruebas rápidas (HTTP):
      python EV_Registry.py
    """
    uvicorn.run(app, host="0.0.0.0", port=7070, log_level="info")

if __name__ == "__main__":
    main()
