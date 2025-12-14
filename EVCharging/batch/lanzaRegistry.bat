@echo off
echo Iniciando EV_Registry con Uvicorn...

cd /d D:\sd1\ds1\ds1\EVCharging\EV_Registry

python -m uvicorn EV_Registry:app ^
  --host 0.0.0.0 ^
  --port 7070 ^
  --ssl-keyfile "D:\sd1\ds1\ds1\EVCharging\EV_Registry\key.pem" ^
  --ssl-certfile "D:\sd1\ds1\ds1\EVCharging\EV_Registry\cert.pem"
pause
