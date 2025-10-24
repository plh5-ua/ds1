import sys
import socket
import threading
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from keep_alive import keep_alive

def handle_client(conn, addr):
    """Atiende a un cliente (por ejemplo, un EV_CP_M) conectado."""
    print(f"[CENTRAL] Nueva conexión desde {addr}")

    try:
        data = conn.recv(1024).decode('utf-8')
        if data:
            print(f"[CENTRAL] Mensaje recibido: {data}")

            # Simula parseo de un mensaje tipo REGISTER_CP#ID#Ubicacion
            if data.startswith("REGISTER_CP"):
                _, cp_id, ubicacion = data.strip().split("#")
                print(f"[CENTRAL] Registrando CP: {cp_id} en {ubicacion}")
                conn.send("ACK".encode('utf-8'))
            else:
                print("[CENTRAL] Mensaje no reconocido")
                conn.send("NACK".encode('utf-8'))
    except Exception as e:
        print(f"[CENTRAL] Error con cliente {addr}: {e}")
    finally:
        conn.close()
        print(f"[CENTRAL] Conexión cerrada con {addr}")

def start_server(puerto):
    """Inicia el servidor socket que escucha a los CPs."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', int(puerto)))
    server.listen(5)
    print(f"[CENTRAL] Servidor escuchando en puerto {puerto}...")

    try:
        while True:
            conn, addr = server.accept()
            hilo = threading.Thread(target=handle_client, args=(conn, addr))
            hilo.start()
    except KeyboardInterrupt:
        print("\n[CENTRAL] Apagando servidor...")
    finally:
        server.close()

def main():
    if len(sys.argv) < 3:
        print("Uso: python EV_Central.py <puerto_escucha> <broker_ip:puerto> [<bd_ip:puerto>]")
        sys.exit(1)

    puerto_escucha = sys.argv[1]
    broker = sys.argv[2]
    bd = sys.argv[3] if len(sys.argv) > 3 else "Sin BD configurada"

    print("────────────────────────────────────────────")
    print("🚀 Iniciando EV_Central (Servidor)")
    print(f"  Puerto de escucha: {puerto_escucha}")
    print(f"  Broker Kafka:      {broker}")
    print(f"  Base de datos:     {bd}")
    print("────────────────────────────────────────────")

    start_server(puerto_escucha)

if __name__ == "__main__":
    main()
