import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from keep_alive import keep_alive

def main():
    if len(sys.argv) < 2:
        print("Uso: python EV_CP_E.py <broker_ip:puerto>")
        sys.exit(1)

    broker = sys.argv[1]
    print("────────────────────────────────────────────")
    print("⚡ Iniciando EV_CP_E (Engine del punto de recarga)")
    print(f"  Conectando a broker Kafka: {broker}")
    print("────────────────────────────────────────────")

    keep_alive("EV_CP_E")

if __name__ == "__main__":
    main()
