@echo off
setlocal EnableDelayedExpansion

rem === CONFIGURACIÓN ===
set "BASE_PORT=6000"
set "BROKER=127.0.0.1:9000"
set "SCRIPT=D:\sd1\ds1\ds1\EVCharging\EV_CP_M\EV_CP_M.py"
set "REGISTRY=127.0.0.1:7070"

rem === LISTA DE PARKINGS (CADA UNO ENTRE COMILLAS) ===
set names="Alicante,ES" "Oslo,NO" "Barcelona,ES"

set prices[1]=0.45
set prices[2]=0.50
set prices[3]=0.49

set count=0
for %%A in (%names%) do (
    set /a count+=1
    set /a port=BASE_PORT+count

    call set "price=%%prices[!count!]%%"
    set "location=%%~A"

    echo ==============================
    echo Lanzando !location!
    echo   -> Puerto: !port!
    echo   -> Precio: !price!
    echo ==============================

    start "CP_!count!_!location!" cmd /k ^
    python "%SCRIPT%" 127.0.0.1:!port! %BROKER% %REGISTRY% !count! "!location!" !price!

    timeout /t 3 /nobreak >nul
)

endlocal
