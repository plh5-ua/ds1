import sys
import socket
import time
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from keep_alive import keep_alive

def main():
    if len(sys.argv) < 4:
        print("Uso: python EV_CP_M.py <ip:puerto_EV_CP_E> <ip:puerto_EV_Central> <id_CP>")
        sys.exit(1)

    ip_cp_e = sys.argv[1]
    ip_central = sys.argv[2]
    id_cp = sys.argv[3]

    print("────────────────────────────────────────────")
    print("🧩 Iniciando EV_CP_M (Monitor)")
    print(f"  EV_CP_E destino:   {ip_cp_e}")
    print(f"  EV_Central destino: {ip_central}")
    print(f"  ID del CP:         {id_cp}")
    print("────────────────────────────────────────────")

    # Separamos IP y puerto de la central
    central_ip, central_port = ip_central.split(":")

    try:
        # Conectar con la central
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            print(f"[{id_cp}] Conectando con la central en {central_ip}:{central_port} ...")
            s.connect((central_ip, int(central_port)))

            # Enviar mensaje de registro
            ubicacion = "Zona_Norte"
            mensaje = f"REGISTER_CP#{id_cp}#{ubicacion}"
            s.send(mensaje.encode('utf-8'))
            print(f"[{id_cp}] Enviado: {mensaje}")

            # Esperar respuesta
            respuesta = s.recv(1024).decode('utf-8')
            print(f"[{id_cp}] Respuesta de la central: {respuesta}")

    except ConnectionRefusedError:
        print(f"[{id_cp}] ❌ No se pudo conectar con la central ({central_ip}:{central_port})")
    except Exception as e:
        print(f"[{id_cp}] Error: {e}")

if __name__ == "__main__":
    main()
