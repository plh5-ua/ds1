import sys
import time
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from keep_alive import keep_alive

def main():
    if len(sys.argv) < 3:
        print("Uso: python EV_Central.py <puerto_escucha> <broker_ip:puerto> [<bd_ip:puerto>]")
        sys.exit(1)

    puerto_escucha = sys.argv[1]
    broker = sys.argv[2]
    bd = sys.argv[3] if len(sys.argv) > 3 else "Sin BD configurada"

    print("────────────────────────────────────────────")
    print("🚀 Iniciando EV_Central")
    print(f"  Puerto de escucha: {puerto_escucha}")
    print(f"  Broker Kafka:      {broker}")
    print(f"  Base de datos:     {bd}")
    print("────────────────────────────────────────────")

    keep_alive("EV_Central")

if __name__ == "__main__":
    main()
