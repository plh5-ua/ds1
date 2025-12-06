@echo off
setlocal

rem === Cambia de directorio ===
cd /d "%KAFKA_HOME%"
%KAFKA_HOME%\bin\windows\kafka-server-start.bat .\config\server.properties
echo ==============================
echo Lanzando KAFKA
echo ==============================

rem === Ejecuta en nueva consola con título ===
start "EV_Central" cmd /k %KAFKA_HOME%\bin\windows\kafka-server-start.bat .\config\server.properties


endlocal
