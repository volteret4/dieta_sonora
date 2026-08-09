#!/usr/bin/env python3
"""
Genre Enricher
Reads albums already synced into music_stats.db by cal_to_estadisticas.py
(release_date/listened_date come from Radicale there) and fills in genre_id
via MusicBrainz/Last.fm for whichever ones don't have one yet, then exports
data.json for the dashboard.

Usage:
    pip install requests
    python extraer_estadisticas.py               # fetch genres + export
    python extraer_estadisticas.py --export-only  # just export, no API calls
"""
import argparse
import sqlite3
import json
import time
import re
import os
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv
import requests

load_dotenv()

# ─────────────────────────────────────────────
#  CONFIGURATION — edit these values
# ─────────────────────────────────────────────
DB_PATH           = "music_stats.db"
JSON_PATH         = "data.json"  # nombre que de verdad fetchea la web (antes "stats.json", nunca lo leía nadie)

MUSICBRAINZ_UA    = "MusicCalendarExtractor/1.0 (your@email.com)"
MB_RATE_LIMIT     = 1.1   # seconds between MusicBrainz requests

LASTFM_API_KEY    = os.getenv("LASTFM_API_KEY")

# Tags that are release types or too generic to be useful as genres
GENRE_BLACKLIST = {
    "album", "single", "ep", "live", "compilation", "soundtrack",
    "electronic",  # too broad — Last.fm will give something more specific
    "pop", "rock",  # only block if you prefer MB sub-genres; remove if you want these
}

# ─────────────────────────────────────────────
#  DATABASE SETUP
# ─────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    name_normalized TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS genres (
    genre_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT NOT NULL,
    name_normalized TEXT NOT NULL UNIQUE
);

-- Which genres are associated with each artist (many-to-many)
CREATE TABLE IF NOT EXISTS artist_genres (
    artist_id   INTEGER NOT NULL REFERENCES artists(artist_id),
    genre_id    INTEGER NOT NULL REFERENCES genres(genre_id),
    PRIMARY KEY (artist_id, genre_id)
);

CREATE TABLE IF NOT EXISTS albums (
    album_id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id                 INTEGER NOT NULL REFERENCES artists(artist_id),
    genre_id                  INTEGER REFERENCES genres(genre_id),
    name                      TEXT NOT NULL,
    name_normalized           TEXT NOT NULL,
    release_date              TEXT,
    listened_date             TEXT,
    days_release_to_listened  INTEGER,
    UNIQUE(artist_id, name_normalized)
);
"""

def _normalize(s: str) -> str:
    """
    Lowercase + collapse whitespace + strip accents for dedup comparisons.
    This avoids creating duplicate rows when the same album appears with
    slightly different accent encoding in CalDAV vs the CSV.
    """
    import unicodedata
    s = re.sub(r'\s+', ' ', s.strip().lower())
    # Decompose accented chars (e.g. é → e + combining accent) then drop combining marks
    s = unicodedata.normalize('NFD', s)
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    return s


def init_db(conn: sqlite3.Connection):
    conn.executescript(SCHEMA)
    # Migración aditiva sobre DBs ya desplegadas -- CREATE TABLE IF NOT EXISTS
    # no toca una tabla existente. Las columnas de compra/tienda de versiones
    # anteriores (purchase_date, purchase_date_estimated,
    # days_release_to_purchase, days_purchase_to_listened) se dejan huérfanas
    # en vez de borrarlas: DROP COLUMN es más arriesgado que no tocarlas.
    cols = {row[1] for row in conn.execute("PRAGMA table_info(albums)")}
    if "days_release_to_listened" not in cols:
        conn.execute(
            "ALTER TABLE albums ADD COLUMN days_release_to_listened INTEGER"
        )
        conn.execute(
            "UPDATE albums SET days_release_to_listened = "
            "CAST(julianday(listened_date) - julianday(release_date) AS INTEGER) "
            "WHERE release_date IS NOT NULL AND listened_date IS NOT NULL"
        )
    conn.commit()


# ─────────────────────────────────────────────
#  NORMALIZED LOOKUP / INSERT HELPERS
# ─────────────────────────────────────────────

def get_or_create_artist(conn: sqlite3.Connection, name: str) -> int:
    """Return artist_id, creating the artist row if it doesn't exist."""
    key = _normalize(name)
    row = conn.execute(
        "SELECT artist_id FROM artists WHERE name_normalized = ?", (key,)
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO artists (name, name_normalized) VALUES (?, ?)", (name, key)
    )
    return cur.lastrowid


def get_or_create_genre(conn: sqlite3.Connection, name: str) -> int:
    """Return genre_id, creating the genre row if it doesn't exist."""
    key = _normalize(name)
    row = conn.execute(
        "SELECT genre_id FROM genres WHERE name_normalized = ?", (key,)
    ).fetchone()
    if row:
        return row[0]
    cur = conn.execute(
        "INSERT INTO genres (name, name_normalized) VALUES (?, ?)", (name, key)
    )
    return cur.lastrowid


def link_artist_genre(conn: sqlite3.Connection, artist_id: int, genre_id: int):
    """Associate an artist with a genre (idempotent)."""
    conn.execute(
        "INSERT OR IGNORE INTO artist_genres (artist_id, genre_id) VALUES (?, ?)",
        (artist_id, genre_id)
    )

# ─────────────────────────────────────────────
#  MUSICBRAINZ GENRE LOOKUP
# ─────────────────────────────────────────────

_mb_cache = {}

def _is_blacklisted(genre: Optional[str]) -> bool:
    """Return True if the genre value should be discarded."""
    if not genre:
        return True
    return genre.lower().strip() in GENRE_BLACKLIST


def _lastfm_get(params: dict) -> Optional[dict]:
    """
    Make a Last.fm API request.
    IMPORTANT: Last.fm always returns HTTP 200, even for errors.
    We must check for the 'error' key in the JSON body instead.
    Returns the parsed JSON dict, or None if there was an API/network error.
    """
    base_params = {
        "api_key": LASTFM_API_KEY,
        "format":  "json",
        "autocorrect": "1",   # string "1", not int, to be safe
    }
    base_params.update(params)
    r = requests.get(
        "https://ws.audioscrobbler.com/2.0/",
        params=base_params,
        timeout=15,
    )
    try:
        data = r.json()
    except Exception as e:
        print(f"    ⚠ Last.fm JSON parse error: {e} — body: {r.text[:200]}")
        return None
    if not isinstance(data, dict):
        print(f"    ⚠ Last.fm returned unexpected type {type(data)}: {str(data)[:200]}")
        return None
    if "error" in data:
        code = data.get("error")
        msg  = data.get("message", "")
        print(f"    ⚠ Last.fm API error {code}: {msg}")
        return None
    return data


def _normalize_tags(raw) -> list[str]:
    """
    Last.fm is inconsistent about tag shape depending on the number of results:
      - 0 tags → {}  or  []  or absent key
      - 1 tag  → {"name": "...", "url": "..."}   (dict, not list)
                 OR just "string"                 (rare, seen with classical)
      - N tags → [{"name": "...", ...}, ...]      (list of dicts)
    Returns a flat list of tag name strings.
    """
    if not raw:
        return []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, dict):
        name = raw.get("name", "")
        return [name] if name else []
    # list — each item may itself be a dict or string
    names = []
    for item in raw:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, dict):
            n = item.get("name", "")
            if n:
                names.append(n)
    return names


def _get_genre_lastfm(artist: str, album: str) -> Optional[str]:
    """
    Query Last.fm for genre tags, skipping blacklisted ones.
    Strategy:
      1. album.getInfo  → toptags of the specific album
      2. artist.getTopTags → if album tags are all blacklisted or absent
    Requires LASTFM_API_KEY to be set.
    """
    if not LASTFM_API_KEY or LASTFM_API_KEY == "TU_API_KEY_LASTFM":
        return None

    try:
        # 1. Album tags
        time.sleep(0.25)
        data = _lastfm_get({"method": "album.getInfo", "artist": artist, "album": album})
        if data:
            album_obj = data.get("album")
            if isinstance(album_obj, dict):
                tags_obj = album_obj.get("tags")
                if isinstance(tags_obj, dict):
                    raw = tags_obj.get("tag", [])
                elif isinstance(tags_obj, list):
                    raw = tags_obj
                else:
                    raw = []
                for name in _normalize_tags(raw):
                    if name.strip() and not _is_blacklisted(name.strip()):
                        return name.strip().title()

        # 2. Artist top tags fallback
        time.sleep(0.25)
        data2 = _lastfm_get({"method": "artist.getTopTags", "artist": artist})
        if data2:
            toptags_obj = data2.get("toptags")
            if isinstance(toptags_obj, dict):
                raw2 = toptags_obj.get("tag", [])
            elif isinstance(toptags_obj, list):
                raw2 = toptags_obj
            else:
                raw2 = []
            for name in _normalize_tags(raw2):
                if name.strip() and not _is_blacklisted(name.strip()):
                    return name.strip().title()

    except Exception as e:
        print(f"    ⚠ Last.fm error for {artist} / {album}: {e}")

    return None


def get_genre_from_musicbrainz(artist: str, album: str) -> Optional[str]:
    """
    1. Query MusicBrainz for tags.
    2. Skip blacklisted values.
    3. If result is blacklisted (or absent), fall back to Last.fm.
    """
    key = (artist.lower(), album.lower())
    if key in _mb_cache:
        return _mb_cache[key]

    time.sleep(MB_RATE_LIMIT)

    genre = None
    try:
        headers = {"User-Agent": MUSICBRAINZ_UA}
        r = requests.get(
            "https://musicbrainz.org/ws/2/release-group",
            params={"query": f'release:"{album}" AND artist:"{artist}"', "fmt": "json", "limit": 5},
            headers=headers, timeout=15,
        )
        r.raise_for_status()
        rgs = r.json().get("release-groups", [])

        if rgs:
            rg_id = rgs[0].get("id")
            time.sleep(MB_RATE_LIMIT)
            r2 = requests.get(
                f"https://musicbrainz.org/ws/2/release-group/{rg_id}",
                params={"inc": "tags", "fmt": "json"},
                headers=headers, timeout=15,
            )
            r2.raise_for_status()
            rg_data = r2.json()

            tags = rg_data.get("tags", [])
            tags_sorted = sorted(tags, key=lambda t: t.get("count", 0), reverse=True)

            # Pick first non-blacklisted tag
            for tag in tags_sorted:
                candidate = tag.get("name", "").strip()
                if not _is_blacklisted(candidate):
                    genre = candidate.title()
                    break

            # If no good tag, check primary-type (but skip release-type values)
            if not genre:
                ptype = rgs[0].get("primary-type", "")
                if ptype and not _is_blacklisted(ptype):
                    genre = ptype.title()

    except Exception as e:
        print(f"  ⚠ MusicBrainz error for {artist} / {album}: {e}")

    # Fallback to Last.fm if MB gave nothing useful
    if not genre:
        print(f"    ↳ MB gave no usable genre, trying Last.fm…")
        genre = _get_genre_lastfm(artist, album)

    source = "MB" if genre and not _is_blacklisted(genre) else "Last.fm" if genre else "—"
    print(f"    🎵 {artist} — {album}: {genre or '(sin género)'} [{source}]")

    _mb_cache[key] = genre
    return genre

# ─────────────────────────────────────────────
#  JSON EXPORT
# ─────────────────────────────────────────────

def export_json(conn: sqlite3.Connection, path: str):
    """
    Export a denormalized view for the dashboard.
    Joins artists + genres so the HTML only needs data.json.
    """
    cur = conn.execute("""
        SELECT
            al.album_id,
            ar.artist_id,
            ar.name                       AS artist,
            al.name                       AS album,
            g.name                        AS genre,
            al.release_date,
            al.listened_date,
            al.days_release_to_listened
        FROM   albums  al
        JOIN   artists ar ON ar.artist_id = al.artist_id
        LEFT   JOIN genres  g  ON g.genre_id  = al.genre_id
        ORDER  BY al.release_date DESC NULLS LAST
    """)
    cols = [d[0] for d in cur.description]
    albums = [dict(zip(cols, row)) for row in cur.fetchall()]

    # Also export artists + genre lists for potential future use
    artists_cur = conn.execute("""
        SELECT ar.artist_id, ar.name,
               GROUP_CONCAT(g.name, ', ') AS genres
        FROM   artists ar
        LEFT JOIN artist_genres ag ON ag.artist_id = ar.artist_id
        LEFT JOIN genres g         ON g.genre_id   = ag.genre_id
        GROUP BY ar.artist_id
        ORDER BY ar.name
    """)
    artists = [dict(zip([d[0] for d in artists_cur.description], row))
               for row in artists_cur.fetchall()]

    genres_cur = conn.execute("""
        SELECT g.genre_id, g.name,
               GROUP_CONCAT(ar.name, ', ') AS artists
        FROM   genres g
        LEFT JOIN artist_genres ag ON ag.genre_id  = g.genre_id
        LEFT JOIN artists ar       ON ar.artist_id = ag.artist_id
        GROUP BY g.genre_id
        ORDER BY g.name
    """)
    genres = [dict(zip([d[0] for d in genres_cur.description], row))
              for row in genres_cur.fetchall()]

    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "albums":       albums,
            "artists":      artists,
            "genres":       genres,
            "generated_at": datetime.now().isoformat(),
        }, f, ensure_ascii=False, indent=2)

    print(f"  Exported {len(albums)} albums · {len(artists)} artists · {len(genres)} genres → {path}")


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Extrae datos del calendario a music_stats.db y exporta data.json para la web."
    )
    parser.add_argument(
        "--export-only", action="store_true",
        help="No vuelve a consultar CalDAV/MusicBrainz: solo exporta data.json desde "
             "music_stats.db tal como está (usado por el orquestador, tras "
             "cal_to_estadisticas.py que ya dejó la DB al día)."
    )
    args = parser.parse_args()

    print("🎵 Music Calendar Extractor")
    print("=" * 40)

    print(f"\n💾 Abriendo {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    init_db(conn)

    if args.export_only:
        print(f"\n📤 Exportando {JSON_PATH}...")
        export_json(conn, JSON_PATH)
        conn.close()
        print("\n✅ ¡Listo!")
        return

    # Álbumes sin género: cal_to_estadisticas.py ya dejó release_date/
    # listened_date al día vía Radicale, así que aquí no hace falta volver a
    # tocar CalDAV -- solo enriquecer género para lo que aún no lo tenga.
    rows = conn.execute("""
        SELECT al.album_id, ar.artist_id, ar.name AS artist, al.name AS album
        FROM   albums al
        JOIN   artists ar ON ar.artist_id = al.artist_id
        WHERE  al.genre_id IS NULL
    """).fetchall()
    print(f"\n🌐 Buscando género para {len(rows)} álbum(es) sin género...")

    stats = {"found": 0, "not_found": 0}
    for album_id, artist_id, artist, album in rows:
        genre = get_genre_from_musicbrainz(artist, album)
        if not genre:
            stats["not_found"] += 1
            continue
        genre_id = get_or_create_genre(conn, genre)
        link_artist_genre(conn, artist_id, genre_id)
        conn.execute("UPDATE albums SET genre_id = ? WHERE album_id = ?", (genre_id, album_id))
        conn.commit()
        stats["found"] += 1

    print(f"  Con género: {stats['found']}  |  Sin género: {stats['not_found']}")

    # JSON export for dashboard
    print(f"\n📤 Exporting {JSON_PATH}...")
    export_json(conn, JSON_PATH)
    conn.close()

    print("\n✅ Done! Open estadisticas.html in your browser.")

if __name__ == "__main__":
    main()
