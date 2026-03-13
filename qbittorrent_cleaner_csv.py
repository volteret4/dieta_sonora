import csv
import os
import argparse
from qbittorrentapi import Client
from sops_env import load_sops_env

load_sops_env()

# Configuración de conexión
QB_HOST = os.getenv("QB_HOST", "localhost")
QB_PORT = os.getenv("QB_PORT", "8080")
QB_USER = os.getenv("QB_USER", "admin")
QB_PASS = os.getenv("QB_PASS", "adminadmin")

def check_albums_in_qb(clean_mode=False):
    qbt_client = Client(host=QB_HOST, port=QB_PORT, username=QB_USER, password=QB_PASS)

    try:
        qbt_client.auth_log_in()
    except Exception as e:
        print(f"Error al conectar: {e}")
        return

    torrents = qbt_client.torrents_info()
    
    albums_restantes = []
    csv_filename = 'albums.csv'

    if not os.path.exists(csv_filename):
        print(f"Error: No se encuentra el archivo {csv_filename}")
        return

    with open(csv_filename, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        
        for row in reader:
            artist = row['artist'].strip().lower()
            album = row['album'].strip().lower()
            
            found = False
            for t in torrents:
                t_name = t.name.lower()
                if artist in t_name and album in t_name:
                    found = True
                    break
            
            if found:
                print(f"[ENCONTRADO - ELIMINANDO] {row['artist']} - {row['album']}")
            else:
                # Si NO está, lo mantenemos en nuestra lista de "pendientes"
                albums_restantes.append(row)
                if not clean_mode:
                    print(f"[FALTA] {row['artist']} - {row['album']}")

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
