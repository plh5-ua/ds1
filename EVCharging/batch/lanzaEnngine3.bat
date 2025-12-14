@echo off
setlocal

rem === CONFIG ===
set "BROKER=192.168.56.1:9092"
set "SCRIPT=D:\sd1\ds1\ds1\EVCharging\EV_CP_E\EV_CP_E.py"

for %%U in (6001 6002 6003) do (
  start "EV_CP_E %%U" cmd /k python "%SCRIPT%" "%BROKER%" %%U 
  timeout /t 1 /nobreak >nul
)

endlocal
