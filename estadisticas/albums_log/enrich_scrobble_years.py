#!/usr/bin/env python3
"""
Rellena scrobble_album_years (en music_stats.db) con el año de
lanzamiento de los álbumes que aparecen en el historial de scrobbles de
Last.fm (lastfm_stats.db) -- no solo los ~745 álbumes ya trackeados vía
Radicale en music_stats.db, sino TODO lo que se ha escuchado alguna vez
(12712 álbumes distintos en producción). Alimenta la gráfica "música
antigua vs actual por mes" del dashboard (ver export_json() en
extraer_estadisticas.py).

Por qué un script aparte y no reutilizar get_genre_from_musicbrainz():
esa función busca género (tags) para el universo mucho más pequeño de
music_stats.db; aquí hace falta el año de lanzamiento (first-release-date,
que ya viene en la misma respuesta de búsqueda de MusicBrainz, sin
petición extra) para un universo ~17x mayor -- backfillear el historial
completo tardaría ~4h al ritmo de rate-limit de MusicBrainz (1.1s/consulta),
así que se procesa incrementalmente por popularidad (álbumes más
escuchados primero) y con límite por ejecución, pensado para un cron
diario propio (ver docker-compose.yml).

Uso:
    python3 enrich_scrobble_years.py --limit 2000
"""
import argparse
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv

_HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_HERE, ".env"))
load_dotenv(os.path.join(_HERE, "..", ".env"))

LASTFM_DB = os.getenv("LASTFM_DB", os.path.join(_HERE, "lastfm_stats.db"))
DB_PATH = os.getenv("MUSIC_DB", os.path.join(_HERE, "music_stats.db"))

MUSICBRAINZ_UA = "MusicCalendarExtractor/1.0 (your@email.com)"
MB_RATE_LIMIT = 1.1  # seconds between MusicBrainz requests -- mismo límite que extraer_estadisticas.py

SCHEMA = """
CREATE TABLE IF NOT EXISTS scrobble_album_years (
    lastfm_album_id  INTEGER PRIMARY KEY,
    artist           TEXT NOT NULL,
    album            TEXT NOT NULL,
    release_year     INTEGER,
    fetched_at       TEXT NOT NULL
);
"""


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.commit()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_release_year_from_musicbrainz(artist: str, album: str) -> Optional[int]:
    """Busca el release-group en MusicBrainz y devuelve el año de
    first-release-date del primer resultado, si lo hay -- ese campo ya
    viene en la respuesta de búsqueda, sin petición de detalle extra.

    Deja que un fallo de red/HTTP (timeout, 503 de MusicBrainz bajo
    carga, etc.) se propague como excepción en vez de devolver None --
    el llamador lo trata como "reintentar la próxima vez", no como
    "MusicBrainz no tiene el dato" (ver main(): solo se cachea
    release_year=None cuando la consulta respondió pero sin resultado
    usable, nunca cuando falló la petición)."""
    time.sleep(MB_RATE_LIMIT)
    r = requests.get(
        "https://musicbrainz.org/ws/2/release-group",
        params={"query": f'release:"{album}" AND artist:"{artist}"', "fmt": "json", "limit": 1},
        headers={"User-Agent": MUSICBRAINZ_UA}, timeout=15,
    )
    r.raise_for_status()
    rgs = r.json().get("release-groups", [])
    if not rgs:
        return None
    date_str = rgs[0].get("first-release-date", "")
    if not date_str or len(date_str) < 4:
        return None
    return int(date_str[:4])


def popular_scrobbled_albums(lastfm_conn: sqlite3.Connection) -> list[tuple[int, str, str, int]]:
    """(album_id, artist, album, scrobble_count) de lastfm_stats.db,
    ordenados de más a menos escuchado."""
    rows = lastfm_conn.execute("""
        SELECT al.album_id, ar.name, al.name, COUNT(*) AS n
        FROM   scrobbles s
        JOIN   albums  al ON al.album_id  = s.album_id
        JOIN   artists ar ON ar.artist_id = al.artist_id
        WHERE  s.album_id IS NOT NULL
        GROUP  BY al.album_id
        ORDER  BY n DESC
    """).fetchall()
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=2000,
                        help="máximo de álbumes nuevos a consultar en MusicBrainz esta ejecución")
    args = parser.parse_args()

    if not os.path.exists(LASTFM_DB):
        print(f"⚠️  Last.fm DB no encontrada en {LASTFM_DB!r} -- nada que hacer")
        return

    # mode=ro no basta -- lastfm_stats.db suele estar en WAL (ver
    # cal_to_estadisticas.py), immutable=1 evita que un lector intente
    # tocar el -shm en un volumen de solo lectura.
    lastfm_conn = sqlite3.connect(f"file:{LASTFM_DB}?mode=ro&immutable=1", uri=True)
    music_conn = sqlite3.connect(DB_PATH)
    init_db(music_conn)

    already_cached = {
        row[0] for row in music_conn.execute("SELECT lastfm_album_id FROM scrobble_album_years")
    }

    candidates = popular_scrobbled_albums(lastfm_conn)
    pending = [c for c in candidates if c[0] not in already_cached]
    print(f"📀 {len(candidates)} álbumes con scrobble, {len(already_cached)} ya en caché, "
          f"{len(pending)} pendientes -- procesando hasta {args.limit}")

    todo = pending[: args.limit]
    found, not_found, errors = 0, 0, 0
    for i, (album_id, artist, album, n) in enumerate(todo, 1):
        try:
            year = get_release_year_from_musicbrainz(artist, album)
        except Exception as e:
            # No se cachea nada -- se reintenta en la próxima ejecución
            # (a diferencia de "MusicBrainz respondió pero sin dato usable",
            # que sí se cachea como release_year=NULL más abajo).
            print(f"  ⚠ error de red para {artist} — {album}: {e}")
            errors += 1
            continue

        music_conn.execute(
            "INSERT INTO scrobble_album_years (lastfm_album_id, artist, album, release_year, fetched_at) "
            "VALUES (?,?,?,?,?)",
            (album_id, artist, album, year, _now()),
        )
        music_conn.commit()
        if year:
            found += 1
            print(f"  [{i}/{len(todo)}] {artist} — {album} ({n} scrobbles): {year}")
        else:
            not_found += 1
        if i % 50 == 0:
            print(f"  … {i}/{len(todo)}")

    print(f"✅ {found} con año, {not_found} sin año, {errors} errores de red (se reintentan) -- "
          f"{len(pending) - len(todo)} quedan para la próxima ejecución")
    lastfm_conn.close()
    music_conn.close()


if __name__ == "__main__":
    main()
