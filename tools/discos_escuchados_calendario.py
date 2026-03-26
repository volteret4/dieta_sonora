#!/usr/bin/env python3
"""
Script para marcar discos escuchados en Radicale basándose en scrobbles de Last.fm

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
from caldav import DAVClient
from icalendar import Calendar
from sops_env import load_sops_env
import os
import pytz

load_sops_env()


# ==================== CONFIGURACIÓN ====================
LASTFM_API_KEY = os.getenv("LASTFM_API_KEY")
LASTFM_USERNAME = os.getenv("LASTFM_USERNAME")
RADICALE_URL = os.getenv("RADICALE_URL")
RADICALE_USERNAME = os.getenv("RADICALE_USERNAME")
RADICALE_PW = os.getenv("RADICALE_PW")
CALENDAR_NAME = os.getenv("CALENDAR_NAME")

# Validar que todas las variables estén configuradas
required_vars = {
    'LASTFM_API_KEY': LASTFM_API_KEY,
    'LASTFM_USERNAME': LASTFM_USERNAME,
    'RADICALE_URL': RADICALE_URL,
    'RADICALE_USERNAME': RADICALE_USERNAME,
    'RADICALE_PW': RADICALE_PW,
    'CALENDAR_NAME': CALENDAR_NAME
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


def get_pending_tasks(client):
    """
    Obtiene todas las tareas pendientes del calendario
    Retorna lista de tuplas (tarea_objeto, artista, álbum)
    """
    try:
        # Conectar al calendario
        principal = client.principal()
        calendars = principal.calendars()

        # Buscar el calendario de tareas
        task_calendar = None
        for cal in calendars:
            # FIX: Convertir URL a string para poder comparar
            cal_url_str = str(cal.url)
            cal_name = cal.name if hasattr(cal, 'name') else ''

            if CALENDAR_NAME in cal_url_str or cal_name == CALENDAR_NAME:
                task_calendar = cal
                break

        if not task_calendar:
            # Intentar con el primer calendario disponible
            if calendars:
                task_calendar = calendars[0]
                print(f"⚠ Calendario '{CALENDAR_NAME}' no encontrado, usando: {task_calendar.name if hasattr(task_calendar, 'name') else 'primer calendario'}")
            else:
                print("✗ No se encontraron calendarios")
                return []

        # Obtener todas las tareas
        todos = task_calendar.todos(include_completed=False)

        pending_tasks = []
        for todo in todos:
            try:
                # Parsear el componente VTODO
                cal = Calendar.from_ical(todo.data)
                for component in cal.walk('VTODO'):
                    summary = str(component.get('summary', ''))

                    # Extraer artista y álbum
                    artist_album = extract_artist_album(summary)
                    if artist_album:
                        pending_tasks.append((todo, artist_album[0], artist_album[1], summary))
            except Exception as e:
                print(f"⚠ Error procesando tarea: {e}")
                continue

        print(f"✓ Encontradas {len(pending_tasks)} tareas de discos pendientes")
        return pending_tasks

    except Exception as e:
        print(f"✗ Error al obtener tareas: {e}")
        import traceback
        traceback.print_exc()  # Mostrar stack trace completo para debug
        return []


def mark_task_completed(todo):
    """
    Marca una tarea como completada con fecha de hoy
    """
    try:
        # Parsear el componente
        cal = Calendar.from_ical(todo.data)

        for component in cal.walk('VTODO'):
            # Marcar como completada
            from icalendar import vDatetime

            component['status'] = 'COMPLETED'
            component['percent-complete'] = 100

            # Usar vDatetime para asegurar la serialización correcta
            completed_dt = datetime.now(pytz.UTC)
            component['completed'] = vDatetime(completed_dt)

        # Guardar los cambios
        todo.data = cal.to_ical()
        todo.save()

        return True

    except Exception as e:
        print(f"✗ Error al marcar tarea como completada: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """
    Función principal
    """
    # Parsear argumentos de línea de comandos
    parser = argparse.ArgumentParser(
        description='Sincroniza scrobbles de Last.fm con tareas en Radicale'
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
    print(f"Sincronización Last.fm → Radicale - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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

    # 2. Conectar a Radicale
    print("\n[2/4] Conectando a Radicale...")
    try:
        client = DAVClient(
            url=RADICALE_URL,
            username=RADICALE_USERNAME,
            password=RADICALE_PW
        )
    except Exception as e:
        print(f"✗ Error al conectar con Radicale: {e}")
        return

    # 3. Obtener tareas pendientes
    print("\n[3/4] Obteniendo tareas pendientes...")
    pending_tasks = get_pending_tasks(client)

    if not pending_tasks:
        print("\n✓ No hay tareas pendientes para comparar")
        return

    # 4. Comparar y marcar como completadas
    print("\n[4/4] Comparando y marcando tareas completadas...")
    completed_count = 0

    for todo, artist, album, original_summary in pending_tasks:
        # Comprobar si el álbum de la tarea está en los scrobbles
        if DEBUG:
            print(f"\n  Comparando tarea: {original_summary}")
            print(f"    → Normalizada: {artist} - {album}")

        if (artist, album) in scrobbled_normalized:
            print(f"\n  ✓ Coincidencia encontrada: {original_summary}")
            if mark_task_completed(todo):
                completed_count += 1
                print(f"    → Marcada como completada")
            else:
                print(f"    → Error al marcar como completada")
        elif DEBUG:
            print(f"    → No coincide con ningún scrobble")

    # Resumen final
    print("\n" + "=" * 60)
    print(f"Resumen: {completed_count} tarea(s) marcada(s) como completada(s)")
    print("=" * 60)


if __name__ == '__main__':
    main()
