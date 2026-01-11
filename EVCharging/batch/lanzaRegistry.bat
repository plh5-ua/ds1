@echo off
title EV_Registry (HTTPS)

REM Cambiamos al directorio del Registry
cd /d D:\sd1\ds1\ds1\EVCharging\EV_Registry

REM Variables de entorno
set "EV_CENTRAL_BASE=http://127.0.0.1:8081"
set "EV_INTERNAL_TOKEN=CHANGE_ME_INTERNAL_TOKEN"
set "EV_REGISTRY_PORT=7070"

echo Iniciando EV_Registry...
python -m uvicorn EV_Registry:app ^
  --host 0.0.0.0 ^
  --port %EV_REGISTRY_PORT% ^
  --ssl-keyfile "key.pem" ^
  --ssl-certfile "cert.pem"

pause
