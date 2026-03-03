import csv
from qbittorrentapi import Client
import os
from dotenv import load_dotenv

load_dotenv()

# Configuración de conexión
QB_HOST = os.getenv("QB_HOST", "localhost")
QB_PORT = os.getenv("QB_PORT", "8080")
QB_USER = os.getenv("QB_USER", "admin")
QB_PASS = os.getenv("QB_PASS", "adminadmin") # Cambia esto por tus credenciales

def check_albums_in_qb():
    # 1. Conectar a qBittorrent
    qbt_client = Client(host=QB_HOST, port=QB_PORT, username=QB_USER, password=QB_PASS)

    try:
        qbt_client.auth_log_in()
    except Exception as e:
        print(f"Error al conectar: {e}")
        return

    # 2. Obtener la lista de todos los torrents actuales
    # Esto es más eficiente que consultar la API por cada fila del CSV
    torrents = qbt_client.torrents_info()

    # 3. Leer el CSV y comparar
    with open('albums.csv', mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f) # Asume que la primera fila es artist,album,date,date

        print(f"{'ESTADO':<15} | {'BÚSQUEDA'}")
        print("-" * 50)

        for row in reader:
            artist = row['artist'].strip().lower()
            album = row['album'].strip().lower()
            search_str = f"{artist} - {album}"

            found = False
            # Buscamos si el artista y el álbum existen en el nombre de algún torrent
            for t in torrents:
                t_name = t.name.lower()
                if artist in t_name and album in t_name:
                    found = True
                    break

            status = "[ENCONTRADO]" if found else "[FALTA]"
            print(f"{status:<15} | {row['artist']} - {row['album']}")

if __name__ == "__main__":
    check_albums_in_qb()
