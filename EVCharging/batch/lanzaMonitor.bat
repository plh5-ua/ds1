@echo off
setlocal enabledelayedexpansion

rem === CONFIGURACIÓN ===
set "BASE_PORT=6000"
set "BROKER=127.0.0.1:9000"
set "SCRIPT=D:\sd1\ds1\ds1\EVCharging\EV_CP_M\EV_CP_M.py"

rem === LISTA DE PARKINGS Y PRECIOS (mismo orden) ===
set names=Parking_A Parking_B Parking_C Parking_D Parking_E
set prices[1]=0.45
set prices[2]=0.50
set prices[3]=0.49
set prices[4]=0.35
set prices[5]=0.43

set count=0
for %%A in (%names%) do (
    set /a count+=1
    set /a port=!BASE_PORT!+!count!

    rem ⚙️ Expansión indirecta correcta del precio
    call set "price=%%prices[!count!]%%"

    echo ==============================
    echo Lanzando %%A
    echo   → Puerto: !port!
    echo   → Precio: !price!
    echo ==============================

    echo CMD: python "%SCRIPT%" 127.0.0.1:!port! %BROKER% !count! %%A !price!
    start "%%A" cmd /k python "%SCRIPT%" 127.0.0.1:!port! %BROKER% !count! %%A !price!
    timeout /t 3 /nobreak >nul
)

endlocal
