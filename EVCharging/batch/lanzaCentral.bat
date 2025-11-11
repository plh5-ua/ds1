@echo off
setlocal

rem === CONFIGURACIÓN ===
set "RUTA_CENTRAL=D:\sd1\ds1\ds1\EVCharging\EV_Central"
set "SCRIPT=EV_Central.py"
set "HTTP_PORT=8080"
set "BROKER=192.168.56.1:9092"
set "EXTRA=127.0.0.1:0"

rem === Cambia de directorio ===
cd /d "%RUTA_CENTRAL%"

echo ==============================
echo 🚀 Lanzando EV_Central en %RUTA_CENTRAL%
echo Puerto HTTP: %HTTP_PORT%
echo Broker Kafka: %BROKER%
echo ==============================

rem === Ejecuta en nueva consola con título ===
start "EV_Central" cmd /k python "%SCRIPT%" %HTTP_PORT% %BROKER% %EXTRA%

rem === Espera 1 segundo (opcional) ===
timeout /t 1 /nobreak >nul

endlocal
