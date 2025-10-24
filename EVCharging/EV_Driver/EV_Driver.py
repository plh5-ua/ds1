import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'common'))
from keep_alive import keep_alive

def main():
    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <broker_ip:puerto> <id_cliente>")
        sys.exit(1)

    broker = sys.argv[1]
    id_cliente = sys.argv[2]

    print("────────────────────────────────────────────")
    print("🚗 Iniciando EV_Driver (Aplicación del conductor)")
    print(f"  Broker Kafka: {broker}")
    print(f"  ID del cliente: {id_cliente}")
    print("────────────────────────────────────────────")

    keep_alive("EV_Driver")

if __name__ == "__main__":
    main()
