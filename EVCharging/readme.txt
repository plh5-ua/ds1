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



Despliegue:
#EV_Central:
cd /d D:\sd1\ds1\ds1\EVCharging\EV_Central
python EV_Central.py 8080 192.168.56.1:9092 127.0.0.1:0

# CP Engine
python EV_CP_E.py 192.168.56.1:9092

EV_CP_E:
cd /d D:\sd1\ds1\ds1\EVCharging\EV_CP_E
python EV_CP_E.py 192.168.56.1:9092
EV_CP_M:
cd /d D:\sd1\ds1\ds1\EVCharging\EV_CP_M
python EV_CP_M.py 192.168.56.1:6000 192.168.56.1:8080 CP-001


