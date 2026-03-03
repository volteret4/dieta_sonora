import requests
import csv
import json
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ORPHEUS_APIKEY")
BASE_URL = "https://orpheus.network/ajax.php"
JSON_FILENAME = "resultado_flacs.json"
CSV_FILENAME = "albums.csv"

HEADERS = {
    "Authorization": API_KEY
}




def obtener_keys():
    try:
        r = requests.get(BASE_URL, headers=HEADERS, params={"action": "index"}, timeout=10)
        if r.status_code != 200:
            print(f" Error {r.status_code}: Credenciales inválidas o baneo temporal.")
            return None, None
        data = r.json()
        if data.get("status") != "success":
            print(f" La API respondió error: {data.get('error')}")
            return None, None
        return data["response"]["authkey"], data["response"]["passkey"]
    except Exception as e:
        print(f" Fallo crítico al obtener llaves: {e}")
        return None, None

def buscar_album(artista, album, authkey, passkey):
    resultado_album = {"artist": artista, "album": album, "groups": []}
    try:
        r = requests.get(BASE_URL, headers=HEADERS, params={
            "action": "browse", "artistname": artista, "groupname": album
        }, timeout=10)
        data = r.json()
    except:
        return resultado_album

    if data.get("status") != "success" or not data.get("response"):
        return resultado_album

    for grupo in data["response"]["results"]:
        group_id = grupo["groupId"]
        time.sleep(1.5) # Rate limit preventivo

        try:
            r2 = requests.get(BASE_URL, headers=HEADERS, params={
                "action": "torrentgroup", "id": group_id
            }, timeout=10)
            data2 = r2.json()
            response_data = data2.get("response")
            if not isinstance(response_data, dict): continue

            torrents = response_data.get("torrents", [])
        except:
            continue

        flacs = [t for t in torrents if t.get("format") == "FLAC"]
        if not flacs: continue

        grupo_info = {
            "groupId": group_id,
            "cover": grupo.get("cover"),
            "webUrl": f"https://orpheus.network/torrents.php?id={group_id}",
            "flacCount": len(flacs),
            "torrents": []
        }

        for t in flacs:
            grupo_info["torrents"].append({
                "torrentId": t["id"],
                "media": t.get("media"),
                "encoding": t.get("encoding"),
                "remasterYear": t.get("remasterYear"),
                "remasterTitle": t.get("remasterTitle"),
                "fileCount": t.get("fileCount"),
                "size": t.get("size"),
                "downloadUrl": f"https://orpheus.network/torrents.php?action=download&id={t['id']}&authkey={authkey}&torrent_pass={passkey}&usetoken=1"
            })
        resultado_album["groups"].append(grupo_info)
    return resultado_album

def main():
    # 1. Cargar historial para evitar duplicados
    resultados_totales = []
    procesados_set = set()
    if os.path.exists(JSON_FILENAME):
        with open(JSON_FILENAME, "r", encoding="utf-8") as f:
            resultados_totales = json.load(f)
            for item in resultados_totales:
                procesados_set.add(f"{item['artist'].lower()}|{item['album'].lower()}")

    authkey, passkey = obtener_keys()
    if not authkey: return

    # 2. Leer CSV completo para poder reescribirlo después
    filas_csv = []
    with open(CSV_FILENAME, "r", encoding="utf-8") as csvfile:
        reader = csv.reader(csvfile)
        filas_csv = list(reader)[1:]

    # 3. Procesar y actualizar
    cambios_realizados = False
    fecha_hoy = datetime.now().strftime("%Y-%m-%d")

    for i, fila in enumerate(filas_csv):
        if len(fila) < 2: continue

        artista, album = fila[0].strip(), fila[1].strip()
        # Si ya está en el JSON, saltar
        if f"{artista.lower()}|{album.lower()}" in procesados_set:
            continue

        print(f" Buscando: {artista} - {album}")
        resultado = buscar_album(artista, album, authkey, passkey)

        # Solo guardamos y marcamos si se encontraron grupos/torrents
        if resultado["groups"]:
            resultados_totales.append(resultado)
            # Añadir fecha a la fila del CSV
            if len(fila) == 2:
                filas_csv[i].append(fecha_hoy)
            else:
                filas_csv[i][2] = fecha_hoy

            cambios_realizados = True

            # Guardado incremental del JSON
            with open(JSON_FILENAME, "w", encoding="utf-8") as f:
                json.dump(resultados_totales, f, indent=4, ensure_ascii=False)

            # Guardado incremental del CSV
            with open(CSV_FILENAME, "w", newline='', encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerows(filas_csv)

            print(f"  [+] ¡Encontrado y marcado!")
        else:
            print(f"  [-] Sin resultados FLAC.")

        time.sleep(2) # Pausa amigable entre búsquedas

    print("\n--- Tarea finalizada ---")

if __name__ == "__main__":
    main()
