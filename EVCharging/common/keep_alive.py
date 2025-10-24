import time

def keep_alive(name: str):
    """Mantiene vivo el proceso mostrando un pulso cada cierto tiempo."""
    try:
        while True:
            print(f"[{name}] En ejecución...")
            time.sleep(5)
    except KeyboardInterrupt:
        print(f"[{name}] Finalizado por el usuario.")
