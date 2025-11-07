guia de despliegue:
instalacion:
# librerias
pip install fastapi uvicorn aiokafka pydantic websockets

#BASEDATA
En la carpetanEVCentral hacer desde una terminal 
mi pc:
cd /d D:\sd1\ds1\ds1\EVCharging\EV_Central
sqlite3 evcentral.db < schema.sql
python init_db.py


cd %KAFKA_HOME%
%KAFKA_HOME%\bin\windows\kafka-server-start.bat .\config\server.properties

Despliegue:
#EV_Central:
cd /d D:\sd1\ds1\ds1\EVCharging\EV_Central
python EV_Central.py 8080 192.168.56.1:9092 127.0.0.1:0


EV_CP_E:
cd /d D:\sd1\ds1\ds1\EVCharging\EV_CP_E
python EV_CP_E.py 192.168.56.1:9092 6001

EV_CP_M:
cd /d D:\sd1\ds1\ds1\EVCharging\EV_CP_M


python EV_CP_M.py <ip_engine:puerto> <ip_central:puerto> <id_cp> <location> <price>

python EV_CP_M.py 127.0.0.1:6001 127.0.0.1:9000 1 Parking_A 0.45

EV_DRIVER:
python EV_Driver.py 192.168.56.1:9092 user-124

API:
http://localhost:8080/static/index.html


#scripts de CP autmaticos

start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_CP_E\EV_CP_E.py 192.168.56.1:9092 6001"
start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_CP_E\EV_CP_E.py 192.168.56.1:9092 6002"
start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_CP_E\EV_CP_E.py 192.168.56.1:9092 6003"

start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_CP_M\EV_CP_M.py 127.0.0.1:6001 127.0.0.1:9000 1 Parking_A 0.45"
start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_CP_M\EV_CP_M.py 127.0.0.1:6002 127.0.0.1:9000 2 Parking_B 0.50"
start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_CP_M\EV_CP_M.py 127.0.0.1:6003 127.0.0.1:9000 3 Parking_C 0.49"

start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_DRIVER\EV_Driver.py 192.168.56.1:9092 user-124
start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_DRIVER\EV_Driver.py 192.168.56.1:9092 user-225


start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_DRIVER\EV_Driver.py 192.168.56.1:9092 user-124 D:\sd1\ds1\ds1\EVCharging\EV_Driver\destinos.txt
start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_CP_E\EV_CP_E.py 192.168.56.1:9092 6004"
start cmd /k "python D:\sd1\ds1\ds1\EVCharging\EV_CP_M\EV_CP_M.py 127.0.0.1:6004 127.0.0.1:9000 4 Parking_d 0.59"

D:\sd1\ds1\ds1\EVCharging\EV_Driver\destinos.txt

