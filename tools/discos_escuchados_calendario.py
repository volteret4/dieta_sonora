#!/usr/bin/env python3
"""
Script para marcar discos escuchados basándose en scrobbles de Last.fm

Marca como hechas las tareas «Artista - Disco» que ya has escuchado, en Radicale
(tareas VTODO), en una lista de Google Tasks o en las dos: lo decide
CALENDAR_BACKENDS (ver calendario.py).

Uso:
    python discos_escuchados_calendario.py              # Revisa el último día
    python discos_escuchados_calendario.py --since 7    # Revisa los últimos 7 días

Ejecutar diariamente con crontab para sincronizar automáticamente
"""

import re
import requests
import sys
import argparse
from datetime import datetime, timedelta
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from tools.sops_env import load_sops_env
import pytz

try:
    load_sops_env()
except FileNotFoundError:
    # En Docker las variables llegan ya descifradas vía docker-compose (scripts/up.sh)
    pass

import calendario  # noqa: E402  (después de cargar las variables)


# ==================== CONFIGURACIÓN ====================
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
LASTFM_USERNAME = os.getenv("LASTFM_USERNAME")
# Las de cada destino (Radicale o Google) las pide calendario.py al abrirlo
required_vars = {
    'LASTFM_API_KEY': LASTFM_API_KEY,
    'LASTFM_USERNAME': LASTFM_USERNAME,
}

missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    print(f"✗ Error: Faltan variables de entorno: {', '.join(missing_vars)}")
    print("  Verifica tu archivo .env")
    sys.exit(1)


# ==================== FUNCIONES ====================

def get_scrobbles_since(days=1):
    """
    Obtiene los scrobbles desde hace X días desde Last.fm
    Retorna una lista de tuplas (artista, álbum)

    Args:
        days: Número de días hacia atrás para buscar scrobbles (por defecto 1)
    """
    url = 'http://ws.audioscrobbler.com/2.0/'

    # Timestamp de inicio (hace X días a las 00:00)
    start_date = datetime.now() - timedelta(days=days)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    unix_timestamp = int(start_date.timestamp())

    params = {
        'method': 'user.getrecenttracks',
        'user': LASTFM_USERNAME,
        'api_key': LASTFM_API_KEY,
        'from': unix_timestamp,
        'format': 'json',
        'limit': 200  # Ajusta si escuchas más de 200 canciones al día
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        albums = set()  # Usamos set para evitar duplicados

        if 'recenttracks' in data and 'track' in data['recenttracks']:
            tracks = data['recenttracks']['track']

            # Si solo hay un track, viene como dict en vez de lista
            if isinstance(tracks, dict):
                tracks = [tracks]

            for track in tracks:
                # Ignorar el "now playing" (sin timestamp)
                if '@attr' in track and 'nowplaying' in track['@attr']:
                    continue

                artist = track['artist'].get('#text', '') if isinstance(track['artist'], dict) else track['artist']
                album = track['album'].get('#text', '') if isinstance(track['album'], dict) else track['album']

                # Solo añadir si tiene álbum
                if album and artist:
                    albums.add((artist, album))

        period_text = f"últimos {days} días" if days > 1 else "hoy"
        print(f"✓ Encontrados {len(albums)} álbumes únicos scrobbleados en los {period_text}")
        return list(albums)

    except Exception as e:
        print(f"✗ Error al obtener scrobbles de Last.fm: {e}")
        return []


def normalize_text(text):
    """
    Normaliza el texto eliminando contenido entre paréntesis, corchetes y llaves
    y convirtiendo a minúsculas para comparación
    Maneja tanto grupos cerrados como no cerrados
    """
    # Primero eliminar grupos cerrados (), [], {}
    text = re.sub(r'\s*\([^)]*\)', '', text)
    text = re.sub(r'\s*\[[^\]]*\]', '', text)
    text = re.sub(r'\s*\{[^}]*\}', '', text)

    # Luego eliminar grupos sin cerrar (desde el símbolo de apertura hasta el final)
    # Esto maneja casos como "Album (2024" donde falta el paréntesis de cierre
    text = re.sub(r'\s*\([^)]*$', '', text)  # Paréntesis sin cerrar al final
    text = re.sub(r'\s*\[[^\]]*$', '', text)  # Corchete sin cerrar al final
    text = re.sub(r'\s*\{[^}]*$', '', text)   # Llave sin cerrar al final

    # Limpiar espacios múltiples y convertir a minúsculas
    text = re.sub(r'\s+', ' ', text).strip().lower()

    # Reemplazar '&' por 'and' para consistencia
    text = text.replace('&', ' and ')

    # Eliminar caracteres especiales de puntuación (puntos, comas)
    text = re.sub(r'[^\w\s]', '', text)

    # Limpiar espacios múltiples y convertir a minúsculas
    text = re.sub(r'\s+', ' ', text).strip().lower()


    return text


def normalize_for_comparison(artist, album):
    """
    Normaliza artista y álbum para comparación
    Elimina contenido entre paréntesis, corchetes y llaves,
    y normaliza espacios y mayúsculas/minúsculas
    """
    artist_normalized = normalize_text(artist)
    album_normalized = normalize_text(album)

    return (artist_normalized, album_normalized)


def extract_artist_album(task_summary):
    # Intentar separar por el guion que divide Artista - Álbum
    if ' - ' in task_summary:
        parts = task_summary.split(' - ', 1)
        # Normalizamos individualmente después de separar
        artist = normalize_text(parts[0])
        album = normalize_text(parts[1])
        return (artist, album)
    return None


def get_pending_tasks(tasks):
    """[(tarea, artista, álbum, resumen)] de las tareas pendientes con forma «Artista - Disco»."""
    out = []
    for summary, handle in tasks.pending():
        artist_album = extract_artist_album(summary)
        if artist_album:
            out.append((handle, artist_album[0], artist_album[1], summary))
    print(f"✓ {tasks.label}: {len(out)} tareas de discos pendientes")
    return out


def main():
    """
    Función principal
    """
    # Parsear argumentos de línea de comandos
    parser = argparse.ArgumentParser(
        description='Sincroniza scrobbles de Last.fm con tus tareas de discos (Radicale y/o Google Tasks)'
    )
    parser.add_argument(
        '--since',
        type=int,
        default=1,
        metavar='DÍAS',
        help='Número de días hacia atrás para buscar scrobbles (por defecto: 1)'
    )
    args = parser.parse_args()

    # Validar que el número de días sea positivo
    if args.since < 1:
        print("✗ Error: El número de días debe ser al menos 1")
        sys.exit(1)

    # Modo debug (puedes activarlo con una variable de entorno)
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"

    print("=" * 60)
    print(f"Sincronización Last.fm → tareas de discos - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    period_text = f"últimos {args.since} días" if args.since > 1 else "último día"
    print(f"Período: {period_text}")
    print("=" * 60)

    # 1. Obtener scrobbles
    print(f"\n[1/4] Obteniendo scrobbles de Last.fm ({period_text})...")
    scrobbled_albums = get_scrobbles_since(args.since)

    if not scrobbled_albums:
        print(f"\n✓ No hay scrobbles para procesar en los {period_text}")
        return

    # Normalizar los álbumes scrobbleados usando la misma función que las tareas
    scrobbled_normalized = set()
    for artist, album in scrobbled_albums:
        normalized = normalize_for_comparison(artist, album)
        scrobbled_normalized.add(normalized)

    print(f"\nÁlbumes scrobbleados:")
    for artist, album in sorted(scrobbled_albums):
        normalized = normalize_for_comparison(artist, album)
        if DEBUG:
            print(f"  • {artist} - {album}")
            print(f"    → Normalizado: {normalized[0]} - {normalized[1]}")
        else:
            print(f"  • {artist} - {album}")

    # 2-4. En cada destino: tareas pendientes y marcar las escuchadas
    completed_count, failed = 0, False
    for backend in calendario.backends():
        print(f"\n[2/4] Conectando a {backend}...")
        try:
            tasks = calendario.task_list(backend)
            print("\n[3/4] Obteniendo tareas pendientes...")
            pending_tasks = get_pending_tasks(tasks)
        except Exception as e:  # un destino caído no impide revisar el otro
            print(f"✗ Error con {backend}: {e}")
            failed = True
            continue

        print("\n[4/4] Comparando y marcando tareas completadas...")
        for handle, artist, album, original_summary in pending_tasks:
            if DEBUG:
                print(f"\n  Comparando tarea: {original_summary}")
                print(f"    → Normalizada: {artist} - {album}")
            if (artist, album) in scrobbled_normalized:
                print(f"\n  ✓ Coincidencia encontrada: {original_summary}")
                try:
                    tasks.complete(handle)
                    completed_count += 1
                    print(f"    → Marcada como completada ({tasks.label})")
                except Exception as e:
                    print(f"    → Error al marcar como completada: {e}")
            elif DEBUG:
                print(f"    → No coincide con ningún scrobble")

    # Resumen final
    print("\n" + "=" * 60)
    print(f"Resumen: {completed_count} tarea(s) marcada(s) como completada(s)")
    print("=" * 60)
    if failed:
        sys.exit(1)


if __name__ == '__main__':
    main()
