import sys
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
    print("🧩 Iniciando EV_CP_M (Monitor del punto de recarga)")
    print(f"  EV_CP_E destino:  {ip_cp_e}")
    print(f"  EV_Central destino: {ip_central}")
    print(f"  ID del CP:        {id_cp}")
    print("────────────────────────────────────────────")

    keep_alive("EV_CP_M")

if __name__ == "__main__":
    main()
