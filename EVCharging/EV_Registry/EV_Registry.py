# EV_Registry.py 
import os
import base64
import hashlib
import secrets
import requests
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
import uvicorn

REGISTRY_PORT = int(os.getenv("EV_REGISTRY_PORT", "7070"))

CENTRAL_BASE = os.getenv("EV_CENTRAL_BASE", "http://127.0.0.1:8080")
INTERNAL_TOKEN = os.getenv("EV_INTERNAL_TOKEN", "CHANGE_ME_INTERNAL_TOKEN")

# IMPORTANTE: Central y Registry deben compartir el mismo PEPPER
PEPPER = os.getenv("EV_REGISTRY_PEPPER", "CHANGE_ME")

app = FastAPI(title="EV_Registry", version="2.0 (API-only)")


# ---------------------------------------------------------------------------
# Crypto: Credential hash (PBKDF2 + PEPPER)
# ---------------------------------------------------------------------------
def hash_cred(cred_plain: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac(
        "sha256",
        (cred_plain + PEPPER).encode("utf-8"),
        salt.encode("utf-8"),
        200_000,
    )
    return base64.urlsafe_b64encode(dk).decode().rstrip("=")


def issue_credential_pair() -> tuple[str, str, str]:
    """
    Devuelve:
      - cred_plain (lo recibe el CP y lo usará en AUTH con Central)
      - salt
      - cred_hash (lo guardará Central)
    """
    cred_plain = base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("=")
    salt = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("=")
    cred_hash = hash_cred(cred_plain, salt)
    return cred_plain, salt, cred_hash


# ---------------------------------------------------------------------------
# CENTRAL internal calls
# ---------------------------------------------------------------------------
def _headers():
    return {"X-Internal-Token": INTERNAL_TOKEN}


def central_upsert(cp_id: str, location: str, price: float, ip_reported: str, salt: str, cred_hash: str):
    url = f"{CENTRAL_BASE}/internal/registry/cp/{cp_id}"
    payload = {
        "cp_id": cp_id,
        "location": location,
        "price": price,
        "ip": ip_reported,
        "salt": salt,
        "cred_hash": cred_hash,
        "revoked": 0
    }
    r = requests.put(url, json=payload, headers=_headers(), timeout=4)
    return r


def central_baja(cp_id: str):
    url = f"{CENTRAL_BASE}/internal/registry/cp/{cp_id}"
    r = requests.delete(url, headers=_headers(), timeout=4)
    return r


def central_get_cp(cp_id: str):
    url = f"{CENTRAL_BASE}/internal/registry/cp/{cp_id}"
    r = requests.get(url, headers=_headers(), timeout=4)
    return r


# ---------------------------------------------------------------------------
# API Models
# ---------------------------------------------------------------------------
class AltaReq(BaseModel):
    cp_id: str = Field(..., min_length=1)
    location: str = Field(..., min_length=1)
    price: float = Field(0.30, ge=0.0)
    # opcional: IP que dice el monitor (yo la guardaría, aunque para auditoría
    # la IP real es request.client.host)
    ip: Optional[str] = None


@app.get("/health")
def health():
    return {"ok": True, "service": "EV_Registry", "ts": datetime.now().isoformat(timespec="seconds")}


@app.put("/cp/{cp_id}")
def alta_cp(cp_id: str, payload: AltaReq, request: Request):
    cp_id = cp_id.strip()
    if payload.cp_id.strip() != cp_id:
        raise HTTPException(status_code=400, detail="cp_id del path y body no coinciden")

    location = payload.location.strip()
    price = float(payload.price)

    client_ip = (request.client.host if request.client else None) or "unknown"
    ip_reported = payload.ip or client_ip

    # 1) generar credential y hash+salt
    cred_plain, salt, cred_hash = issue_credential_pair()

    # 2) pedir a CENTRAL que persista CP + credenciales + auditoría
    try:
        r = central_upsert(cp_id, location, price, ip_reported, salt, cred_hash)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Central no accesible: {e}")

    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Central rechazó alta: {r.status_code} {r.text}")

    # 3) devolver credential en claro al CP
    data = r.json() if r.headers.get("content-type","").startswith("application/json") else {}
    return {
        "ok": True,
        "cp": data.get("cp"),
        "credential": cred_plain
    }


@app.delete("/cp/{cp_id}")
def baja_cp(cp_id: str):
    cp_id = cp_id.strip()
    try:
        r = central_baja(cp_id)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Central no accesible: {e}")

    if r.status_code != 200:
        raise HTTPException(status_code=502, detail=f"Central rechazó baja: {r.status_code} {r.text}")

    return r.json()


@app.get("/cp/{cp_id}")
def get_cp_info(cp_id: str):
    cp_id = cp_id.strip()
    try:
        r = central_get_cp(cp_id)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Central no accesible: {e}")

    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)

    return r.json()


if __name__ == "__main__":
    # HTTPS (igual que antes)
    uvicorn.run(
        "EV_Registry:app",
        host="0.0.0.0",
        port=REGISTRY_PORT,
        ssl_certfile="cert.pem",
        ssl_keyfile="key.pem",
    )
