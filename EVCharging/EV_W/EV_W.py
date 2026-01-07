# EV_W.py
# Uso:
#   python EV_W.py <central_base_url> <openweather_api_key> [cities_file]
#
# Ejemplo:
#   python EV_W.py http://127.0.0.1:8080 TU_API_KEY cities.txt
#
# cities.txt (opcional):
#   Alicante,ES
#   Madrid,ES
#   Oslo,NO

import sys, asyncio, json, os
from dataclasses import dataclass
from typing import Dict, Optional, Set
import httpx

POLL_SEC = 4  # requisito: cada 4 segundos :contentReference[oaicite:4]{index=4}
THRESHOLD_C = 0.0

@dataclass
class LocState:
    temp_c: Optional[float] = None
    alert: bool = False

def load_cities(path: str) -> Set[str]:
    if not path or not os.path.exists(path):
        return set()
    out = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            t = line.strip()
            if not t or t.startswith("#"):
                continue
            out.add(t)
    return out

async def geocode_latlon(
    client: httpx.AsyncClient,
    api_key: str,
    city_query: str,
    cache: Dict[str, tuple[float, float]]
) -> tuple[float, float]:
    """
    city_query: "Alicante,ES" recomendado. También vale "Madrid" (pero puede ser ambiguo).
    cache: para no pedir geocoding cada ciclo.
    """
    key = city_query.strip()
    if key in cache:
        return cache[key]

    r = await client.get(
        "https://api.openweathermap.org/geo/1.0/direct",
        params={"q": key, "limit": 1, "appid": api_key},
        timeout=10.0
    )
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError(f"No se encontró geocoding para: {city_query}")

    lat = float(data[0]["lat"])
    lon = float(data[0]["lon"])
    cache[key] = (lat, lon)
    return lat, lon


async def fetch_temp_c(
    client: httpx.AsyncClient,
    api_key: str,
    city_query: str,
    geo_cache: Dict[str, tuple[float, float]]
) -> float:
    lat, lon = await geocode_latlon(client, api_key, city_query, geo_cache)

    r = await client.get(
        "https://api.openweathermap.org/data/2.5/weather",
        params={"lat": lat, "lon": lon, "appid": api_key, "units": "metric"},
        timeout=10.0
    )
    r.raise_for_status()
    data = r.json()
    return float(data["main"]["temp"])

async def notify_central(client: httpx.AsyncClient, central_base: str, city: str, temp_c: float, alert: bool):
    # Contrato propuesto
    payload = {"location": city, "temp_c": temp_c, "alert": alert}
    url = central_base.rstrip("/") + "/weather/alert"
    try:
        r = await client.post(url, json=payload, timeout=5.0)
        r.raise_for_status()
        print(f"→ CENTRAL notified {payload} (ok)")
    except Exception as e:
        print(f"⚠️ Error notificando a Central {url}: {e}")

async def poll_loop(central_base: str, api_key: str, cities: Set[str], states: Dict[str, LocState], stop_evt: asyncio.Event):
    async with httpx.AsyncClient() as client:
        geo_cache: Dict[str, tuple[float, float]] = {}
        while not stop_evt.is_set():
            # snapshot para que el menú pueda modificar cities sin romper iteración
            snapshot = list(cities)
            if not snapshot:
                await asyncio.sleep(0.5)
                continue

            for city in snapshot:
                try:
                    temp_c = await fetch_temp_c(client, api_key, city, geo_cache)
                except Exception as e:
                    print(f"⚠️ OpenWeather fail city={city}: {e}")
                    continue

                st = states.setdefault(city, LocState())
                new_alert = temp_c < THRESHOLD_C

                transition = (st.temp_c is None) or (new_alert != st.alert)

                # actualiza estado local
                st.temp_c = temp_c
                st.alert = new_alert

                # NOTIFICAR SIEMPRE (para actualizar temperatura en Central/front)
                await notify_central(client, central_base, city, temp_c, new_alert)

                # logs bonitos
                if transition:
                    print(f"[ALERTA-TRANSICION] city={city} temp={temp_c:.2f}C alert={new_alert}")
                else:
                    print(f"[OK] city={city} temp={temp_c:.2f}C alert={new_alert}")

            await asyncio.sleep(POLL_SEC)

def print_menu():
    print("\n===== EV_W MENU =====")
    print("1) Listar ciudades")
    print("2) Añadir ciudad")
    print("3) Eliminar ciudad")
    print("4) Ver estados (temp/alert)")
    print("q) Salir")
    print("=====================\n")

async def menu_loop(cities: Set[str], states: Dict[str, LocState], stop_evt: asyncio.Event):
    print_menu()
    while True:
        cmd = (await asyncio.to_thread(input, "EV_W> ")).strip().lower()

        if cmd == "1":
            print("Ciudades:", sorted(cities) if cities else "(vacío)")

        elif cmd == "2":
            c = (await asyncio.to_thread(input, "Ciudad a añadir: ")).strip()
            if c:
                cities.add(c)
                print("OK añadido:", c)

        elif cmd == "3":
            c = (await asyncio.to_thread(input, "Ciudad a eliminar: ")).strip()
            if c in cities:
                cities.remove(c)
                print("OK eliminado:", c)
            else:
                print("No existe en lista.")

        elif cmd == "4":
            for c in sorted(cities):
                st = states.get(c) or LocState()
                print(f"- {c}: temp={st.temp_c} alert={st.alert}")

        elif cmd == "q":
            stop_evt.set()
            return

        else:
            print("Comando no válido.")

        print_menu()


async def main():
    if len(sys.argv) < 3:
        print("Uso: python EV_W.py <central_base_url> <openweather_api_key> [cities_file]")
        sys.exit(1)

    central_base = sys.argv[1]
    api_key = sys.argv[2]
    cities_file = sys.argv[3] if len(sys.argv) >= 4 else None

    cities = load_cities(cities_file) if cities_file else set()
    states: Dict[str, LocState] = {}
    stop_evt = asyncio.Event()

    # Requisito: poder cambiar localizaciones “a voluntad” sin reiniciar
    poll_task = asyncio.create_task(poll_loop(central_base, api_key, cities, states, stop_evt))
    try:
        await menu_loop(cities, states, stop_evt)
    finally:
        stop_evt.set()
        await poll_task

if __name__ == "__main__":
    asyncio.run(main())
