import sqlite3
from dotenv.main import load_dotenv
import requests
import time
import json
import os
from typing import List, Dict, Any, Optional
import dotenv

load_sops_env()

class OrpheusChecker:
    def __init__(self, db_path: str, api_key: str, api_url: str):
        """
        Inicializa el verificador de Orpheus.

        Args:
            db_path: Ruta a la base de datos SQLite
            api_key: API key para Orpheus
            api_url: URL base de la API de Orpheus
        """
        self.db_path = db_path
        self.api_key = api_key
        self.api_url = api_url
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'token {self.api_key}',
            'Content-Type': 'application/json'
        })

    def connect_db(self) -> sqlite3.Connection:
        """Conecta a la base de datos SQLite."""
        return sqlite3.connect(self.db_path)

    def get_albums_from_db(self) -> List[Dict[str, Any]]:
        """Obtiene todos los álbumes de la base de datos local."""
        conn = self.connect_db()
        cursor = conn.cursor()

        query = """
        SELECT a.id, a.name as album_name, art.name as artist_name, a.year
        FROM albums a
        JOIN artists art ON a.artist_id = art.id
        """

        cursor.execute(query)
        albums = []
        for row in cursor.fetchall():
            albums.append({
                'id': row[0],
                'album_name': row[1],
                'artist_name': row[2],
                'year': row[3]
            })

        conn.close()
        return albums

    def search_orpheus(self, artist: str, album: str) -> Optional[Dict[str, Any]]:
        """
        Busca un álbum en Orpheus usando los parámetros específicos de artistname y groupname.

        Args:
            artist: Nombre del artista
            album: Nombre del álbum

        Returns:
            Resultados de la búsqueda o None en caso de error
        """
        try:
            # Construye la consulta usando los parámetros específicos
            params = {
                'action': 'browse',
                'artistname': artist,
                'groupname': album,
                'page': 1
            }

            response = self.session.get(f"{self.api_url}/ajax.php", params=params)
            response.raise_for_status()

            return response.json()
        except requests.RequestException as e:
            print(f"Error al buscar {artist} - {album}: {e}")
            time.sleep(2)  # Pausa para evitar ser bloqueado por demasiadas peticiones
            return None

    def check_album_exists(self, search_results: Dict[str, Any], artist: str, album: str) -> bool:
        """
        Verifica si un álbum específico existe en los resultados de la búsqueda.

        Args:
            search_results: Resultados de la búsqueda de Orpheus
            artist: Nombre del artista a verificar
            album: Nombre del álbum a verificar

        Returns:
            True si el álbum existe, False si no
        """
        #print(search_results)  #debug
        if not search_results or 'status' not in search_results or search_results.get('status') != 'success':
            return False

        # Verificar si hay resultados
        if 'response' not in search_results or 'results' not in search_results['response']:
            return False

        # Si hay algún resultado, significa que se encontró el álbum
        # ya que estamos buscando específicamente por artistname y groupname
        return len(search_results['response']['results']) > 0

    def find_missing_albums(self, limit: int = None) -> List[Dict[str, Any]]:
        """
        Encuentra los álbumes que no existen en Orpheus.

        Args:
            limit: Límite opcional para la cantidad de álbumes a verificar

        Returns:
            Lista de álbumes que no se encontraron en Orpheus
        """
        albums = self.get_albums_from_db()
        if limit:
            albums = albums[:limit]

        missing_albums = []

        for i, album in enumerate(albums):
            print(f"Verificando {i+1}/{len(albums)}: {album['artist_name']} - {album['album_name']}")

            search_results = self.search_orpheus(album['artist_name'], album['album_name'])
            exists = self.check_album_exists(search_results, album['artist_name'], album['album_name'])

            if not exists:
                missing_albums.append(album)
                print(f"No encontrado: {album['artist_name']} - {album['album_name']}")

            # Evitar sobrecargar la API
            time.sleep(1)

        return missing_albums

    def save_missing_albums(self, missing_albums: List[Dict[str, Any]], output_file: str):
        """
        Guarda la lista de álbumes no encontrados en un archivo JSON.

        Args:
            missing_albums: Lista de álbumes no encontrados
            output_file: Ruta del archivo de salida
        """
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(missing_albums, f, ensure_ascii=False, indent=2)

        print(f"Se guardaron {len(missing_albums)} álbumes no encontrados en {output_file}")


def main():
    # Configuración
    db_path = "/home/huan/gits/pollo/music-fuzzy/base_datos/musica.sqlite"
    api_key = os.getenv('ORPHEUS_APIKEY')
    api_url = "https://orpheus.network"
    output_file = "albumenes_no_en_orpheus.json"

    # Verificar si el archivo de config existe
    config_file = "config.json"
    if os.path.exists(config_file):
        with open(config_file, 'r') as f:
            config = json.load(f)
            db_path = config.get('db_path', db_path)
            api_key = config.get('api_key', api_key)
            api_url = config.get('api_url', api_url)

    # Crear el verificador
    checker = OrpheusChecker(db_path, api_key, api_url)

    # Encontrar álbumes faltantes (opcionalmente establece un límite para pruebas)
    # missing_albums = checker.find_missing_albums(limit=10)  # Para pruebas
    missing_albums = checker.find_missing_albums()

    # Guardar resultados
    checker.save_missing_albums(missing_albums, output_file)


if __name__ == "__main__":
    main()
