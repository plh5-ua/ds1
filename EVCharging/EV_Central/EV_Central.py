import sys
import socket
import threading
import os
import time
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from keep_alive import keep_alive

# -------------------------------
# Almacenamiento temporal de CPs
# -------------------------------
registered_cps = {}  # { id_cp: { 'ubicacion': str, 'estado': str, 'ultima_conexion': float } }
lock = threading.Lock()  # para evitar conflictos de acceso concurrente

def mostrar_panel():
    """Muestra la lista de CPs registrados en formato de tabla."""
    os.system('cls' if os.name == 'nt' else 'clear')
    print("╔════════════════════════════════════════════╗")
    print("║       PANEL DE MONITORIZACIÓN CENTRAL      ║")
    print("╚════════════════════════════════════════════╝\n")

    if not registered_cps:
        print("No hay puntos de recarga registrados aún.\n")
    else:
        print(f"{'ID_CP':<10} {'Ubicación':<15} {'Estado':<12} {'Última conexión':<20}")
        print("-" * 60)
        for cp_id, info in registered_cps.items():
            t = time.strftime('%H:%M:%S', time.localtime(info['ultima_conexion']))
            print(f"{cp_id:<10} {info['ubicacion']:<15} {info['estado']:<12} {t:<20}")
    print("\nEsperando conexiones...\n")

def handle_client(conn, addr):
    """Atiende a un cliente (por ejemplo, un EV_CP_M) conectado."""
    try:
        data = conn.recv(1024).decode('utf-8')
        if data:
            print(f"[CENTRAL] Mensaje recibido de {addr}: {data}")

            if data.startswith("REGISTER_CP"):
                _, cp_id, ubicacion = data.strip().split("#")

                with lock:
                    registered_cps[cp_id] = {
                        'ubicacion': ubicacion,
                        'estado': 'ACTIVADO',
                        'ultima_conexion': time.time()
                    }

                conn.send("ACK".encode('utf-8'))
                mostrar_panel()

            else:
                print("[CENTRAL] Mensaje no reconocido")
                conn.send("NACK".encode('utf-8'))

    except Exception as e:
        print(f"[CENTRAL] Error con cliente {addr}: {e}")
    finally:
        conn.close()

def start_server(puerto):
    """Inicia el servidor socket que escucha a los CPs."""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', int(puerto)))
    server.listen(5)
    print(f"[CENTRAL] Servidor escuchando en puerto {puerto}...\n")

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

    mostrar_panel()
    start_server(puerto_escucha)

if __name__ == "__main__":
    main()
