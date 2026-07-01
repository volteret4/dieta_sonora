import csv
import os
import re
import unicodedata
import argparse
from qbittorrentapi import Client
from tools.sops_env import load_sops_env

load_sops_env()

# Configuración de conexión
QB_HOST = os.getenv("QB_HOST", "localhost")
QB_PORT = os.getenv("QB_PORT", "8080")
QB_USER = os.getenv("QB_USER", "admin")
QB_PASS = os.getenv("QB_PASS", "adminadmin")


def _norm(s: str) -> str:
    """Normaliza para comparación: minúsculas, sin acentos, separadores → espacio."""
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = s.lower()
    s = re.sub(r'[_\-\.]+', ' ', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def _album_in_torrent(artist: str, album: str, t_name: str) -> bool:
    na, nb, nt = _norm(artist), _norm(album), _norm(t_name)
    return na in nt and nb in nt


def check_albums_in_qb(clean_mode=False):
    qbt_client = Client(host=QB_HOST, port=QB_PORT, username=QB_USER, password=QB_PASS)

    try:
        qbt_client.auth_log_in()
    except Exception as e:
        print(f"Error al conectar: {e}")
        return

    torrents = qbt_client.torrents_info()
    torrent_names = [t.name for t in torrents]

    albums_restantes = []
    csv_filename = 'albums.csv'

    if not os.path.exists(csv_filename):
        print(f"Error: No se encuentra el archivo {csv_filename}")
        return

    with open(csv_filename, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames

        for row in reader:
            artist = row['artist'].strip()
            album = row['album'].strip()

            found = any(_album_in_torrent(artist, album, t) for t in torrent_names)

            if found:
                print(f"[ENCONTRADO - ELIMINANDO] {artist} - {album}")
            else:
                albums_restantes.append(row)
                if not clean_mode:
                    print(f"[FALTA] {artist} - {album}")

    # Si se activó --clean, sobreescribimos el archivo con lo que NO se encontró
    if clean_mode:
        with open(csv_filename, mode='w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(albums_restantes)
        print(f"\n--- Limpieza completada. Se han mantenido {len(albums_restantes)} álbumes en {csv_filename} ---")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Chequea álbumes en qBittorrent.")
    parser.add_argument('--clean', action='store_true', help="Elimina del CSV los álbumes que ya están descargados.")

    args = parser.parse_args()
    check_albums_in_qb(clean_mode=args.clean)
