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

HEADERS = {"Authorization": API_KEY}

def obtener_datos_usuario():
    """Obtiene el ID de usuario, authkey y passkey."""
    try:
        r = requests.get(BASE_URL, headers=HEADERS, params={"action": "index"}, timeout=10)
        data = r.json()
        if data.get("status") == "success":
            return (
                data["response"]["id"],
                data["response"]["authkey"],
                data["response"]["passkey"]
            )
        return None, None, None
    except Exception as e:
        print(f"Error obteniendo datos de usuario: {e}")
        return None, None, None

def obtener_historial_descargas(user_id):
    """Obtiene la lista de IDs de torrents descargados por el usuario y su fecha."""
    print(" ⏳ Recuperando tu historial de descargas personal...")
    descargas = {}
    try:
        # Nota: Gazelle suele paginar esto. Aquí traemos la primera página (recientes).
        r = requests.get(BASE_URL, headers=HEADERS, params={
            "action": "userhistory",
            "type": "downloads",
            "id": user_id
        }, timeout=15)
        data = r.json()
        if data.get("status") == "success":
            for item in data["response"]["results"]:
                # Guardamos la fecha de la descarga indexada por el ID del torrent
                descargas[str(item["torrentId"])] = item["downloadTime"]
        return descargas
    except Exception as e:
        print(f"Error al obtener historial: {e}")
        return {}

def buscar_fecha_descarga_en_album(resultado_album, historial_usuario):
    """
    Busca si alguno de los torrents del álbum está en el historial del usuario.
    Retorna la fecha más reciente encontrada.
    """
    fechas_encontradas = []
    for grupo in resultado_album.get("groups", []):
        for torrent in grupo.get("torrents", []):
            t_id = str(torrent["torrentId"])
            if t_id in historial_usuario:
                fechas_encontradas.append(historial_usuario[t_id])

    if fechas_encontradas:
        # Retornamos la fecha más reciente de descarga de ese álbum
        return max(fechas_encontradas)
    return "No descargado"

def main():
    user_id, authkey, passkey = obtener_datos_usuario()
    if not user_id:
        print("No se pudo autenticar.")
        return

    # 1. Obtener el historial completo del usuario una sola vez
    historial_usuario = obtener_historial_descargas(user_id)

    # 2. Cargar datos del JSON (donde están los torrentIds)
    if not os.path.exists(JSON_FILENAME):
        print(f"Error: No existe {JSON_FILENAME}. Ejecuta primero la búsqueda de torrents.")
        return

    with open(JSON_FILENAME, "r", encoding="utf-8") as f:
        datos_json = json.load(f)

    # 3. Leer CSV
    with open(CSV_FILENAME, "r", encoding="utf-8") as f:
        reader = list(csv.reader(f))

    # 4. Procesar correspondencias
    filas_finales = []

    # Mapeamos el JSON para búsqueda rápida por artista|album
    mapa_json = {f"{item['artist'].lower()}|{item['album'].lower()}": item for item in datos_json}

    for fila in reader:
        # Aseguramos que la fila tenga al menos 4 columnas (Artista, Album, Detectado, Descargado)
        while len(fila) < 3: fila.append("") # Columna 3: Fecha detección (ya existente)
        if len(fila) < 4: fila.append("")    # Columna 4: Fecha de tu descarga (nueva)

        artista, album = fila[0].strip(), fila[1].strip()
        key = f"{artista.lower()}|{album.lower()}"

        if key in mapa_json:
            resultado_album = mapa_json[key]
            # Buscamos en el historial si bajaste este album
            fecha_descarga = buscar_fecha_descarga_en_album(resultado_album, historial_usuario)

            # Actualizamos JSON con el dato histórico
            resultado_album["user_download_date"] = fecha_descarga

            # Actualizamos la 4ta columna del CSV
            fila[3] = fecha_descarga
            print(f"📊 {artista} - {album}: Descargado el {fecha_descarga}")
        else:
            fila[3] = "Datos no encontrados"

        filas_finales.append(fila)

    # 5. Guardar resultados
    with open(JSON_FILENAME, "w", encoding="utf-8") as f:
        json.dump(datos_json, f, indent=4, ensure_ascii=False)

    with open(CSV_FILENAME, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(filas_finales)

    print("\n✅ Proceso completado. Revisa la cuarta columna de tu CSV.")

if __name__ == "__main__":
    main()
