@echo off
setlocal

rem === CONFIG ===
set "BROKER=192.168.56.1:9092"
set "SCRIPT=D:\sd1\ds1\ds1\EVCharging\EV_DRIVER\EV_Driver.py"
set "DEST=D:\sd1\ds1\ds1\EVCharging\EV_Driver\destinos.txt"

for %%U in (user1 user2 user3 user4 user5) do (
  start "EV_DRIVER %%U" cmd /k python "%SCRIPT%" %BROKER% %%U "%DEST%"
  timeout /t 4 /nobreak >nul
)

endlocal
