#!/usr/bin/env python3
"""
sync_music.py — Script unificado de sincronización musical
===========================================================

Fuente de verdad: los VTODOs del calendario de tareas (CALENDAR_TASKS).
  · SUMMARY   → artist / album
  · COMPLETED → listened_date  (si ya está en el VTODO, no se consulta Last.fm)

No se rastrea fecha de compra/tienda: las estimaciones vía Airsonic/
qBittorrent (fecha "created"/"added_on" de la biblioteca) resultaron
demasiado poco fiables -- solo se sigue Lanzamiento → Escucha.

Flujo:
  1. Lee TODOS los VTODOs del calendario de tareas (CALENDAR_TASKS).
  2. Lee VEVENTs del calendario de lanzamientos (CALENDAR_NAME), solo para
     obtener release_date.
  3. Para cada VTODO (loop principal):
       a. Cruza con VEVENTs para obtener release_date (si no, MusicBrainz).
       b. Actualiza la DB con (release, listened) del VTODO.
       c. Si el VTODO ya tiene COMPLETED → saltar paso Last.fm.
       d. Si no tiene listened_date → busca tracklist en MusicBrainz y compara
          contra lastfm_stats.db; si hay escucha, marca VTODO COMPLETED + actualiza DB.

Uso:
    python sync_music.py              # todos los VTODOs
    python sync_music.py --dry-run    # solo muestra, no escribe nada
    python sync_music.py --auto       # no interactivo (usado por main.sh)

Variables en .env (ubicado junto al script o en el directorio raíz):
    RADICALE_URL        — ej: http://localhost:5232
    RADICALE_USERNAME   — usuario Radicale
    RADICALE_PW         — contraseña Radicale
    RADICALE_CALENDAR   — ruta base del usuario en Radicale (ej: /usuario/)
    CALENDAR_NAME       — nombre/segmento del calendario de lanzamientos (ej: qwer)
    CALENDAR_TASKS      — nombre/segmento del calendario de tareas      (ej: asdf)
    LASTFM_DB           — ruta a lastfm_stats.db  (defecto: lastfm_stats.db)
    MUSIC_DB            — ruta a music_stats.db   (defecto: music_stats.db)
    MB_EMAIL            — email para User-Agent de MusicBrainz
"""

import argparse
import os
import re
import sqlite3
import sys
import time

from datetime import datetime, date, timezone
from typing import Optional
from xml.etree import ElementTree as ET

import requests
from dotenv import load_dotenv
from icalendar import Calendar, vDatetime, vText

# ── Certifi opcional ──────────────────────────────────────────────────────────
try:
    import certifi
    _MB_VERIFY = certifi.where()
except ImportError:
    _MB_VERIFY = True

# ── Cargar .env (busca hacia arriba desde el script) ─────────────────────────
_HERE = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(_HERE, ".env"))
load_dotenv(os.path.join(_HERE, "..", ".env"))  # también raíz del proyecto

# ─────────────────────────────────────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────────────────────────────────────
RADICALE_URL      = os.getenv("RADICALE_URL",      "").rstrip("/")
RADICALE_USER     = os.getenv("RADICALE_USERNAME", "")
RADICALE_PW       = os.getenv("RADICALE_PW",       "")
RADICALE_BASE     = os.getenv("RADICALE_CALENDAR", "/")   # ej: /usuario/
CALENDAR_NAME     = os.getenv("CALENDAR_NAME",     "")    # calendario de lanzamientos
CALENDAR_TASKS    = os.getenv("CALENDAR_TASKS",    "")    # calendario de tareas

LASTFM_DB  = os.getenv("LASTFM_DB",  os.path.join(_HERE, "lastfm_stats.db"))
MUSIC_DB   = os.getenv("MUSIC_DB",   os.path.join(_HERE, "music_stats.db"))
MB_EMAIL   = os.getenv("MB_EMAIL",   "user@example.com")

MB_BASE       = "https://musicbrainz.org/ws/2/"
MB_UA         = f"SyncMusic/2.0 ({MB_EMAIL})"
MB_RATE_LIMIT = 1.5

# Sesión MB persistente (reutiliza conexión TCP/SSL)
_mb_session = requests.Session()
_mb_session.headers.update({"User-Agent": MB_UA})
_mb_session.verify = _MB_VERIFY


# ─────────────────────────────────────────────────────────────────────────────
#  HELPERS GENERALES
# ─────────────────────────────────────────────────────────────────────────────

def _normalize(s: str) -> str:
    import unicodedata
    s = re.sub(r"\s+", " ", s.strip().lower())
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def strip_emojis(s: str) -> str:
    return re.sub(
        r'^[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+'
        r'|[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+$',
        "", s,
    ).strip()


def parse_summary(summary: str) -> tuple[str, str]:
    """'Artist - Album' → (artist, album). Tolera —, –, -."""
    summary = strip_emojis(summary)
    parts = re.split(r"\s+[-–—]\s+", summary, maxsplit=1)
    if len(parts) == 2:
        return strip_emojis(parts[0]), strip_emojis(parts[1])
    return summary, ""


def parse_date_value(dt_val) -> Optional[date]:
    if dt_val is None:
        return None
    if hasattr(dt_val, "dt"):
        dt_val = dt_val.dt
    if isinstance(dt_val, datetime):
        return dt_val.date()
    if isinstance(dt_val, date):
        return dt_val
    return None


def days_between(d1: Optional[str], d2: Optional[str]) -> Optional[int]:
    if not d1 or not d2:
        return None
    try:
        return (date.fromisoformat(d2) - date.fromisoformat(d1)).days
    except ValueError:
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  CALDAV — HELPERS HTTP RAW
# ─────────────────────────────────────────────────────────────────────────────

def _cal_url(cal_name: str) -> str:
    """Construye la URL completa del calendario dado su nombre/segmento."""
    base = RADICALE_BASE.rstrip("/")
    return f"{RADICALE_URL}{base}/{cal_name}/"


def fetch_calendar_items(cal_name: str) -> list[dict]:
    """
    Usa REPORT (calendar-query) para obtener todos los ítems de un calendario.
    Devuelve lista de dicts: {href, ical_text}.
    """
    url = _cal_url(cal_name)
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">'
        "  <D:prop><D:getetag/><C:calendar-data/></D:prop>"
        "  <C:filter><C:comp-filter name=\"VCALENDAR\"/></C:filter>"
        "</C:calendar-query>"
    )
    headers = {"Depth": "1", "Content-Type": "application/xml; charset=utf-8"}
    r = requests.request(
        "REPORT", url,
        data=body.encode("utf-8"),
        headers=headers,
        auth=(RADICALE_USER, RADICALE_PW),
        timeout=30,
    )
    r.raise_for_status()

    ns = {"D": "DAV:", "C": "urn:ietf:params:xml:ns:caldav"}
    root = ET.fromstring(r.content)
    items = []
    for resp in root.findall(".//D:response", ns):
        href_el  = resp.find("D:href", ns)
        cal_data = resp.find(".//C:calendar-data", ns)
        if href_el is not None and cal_data is not None and cal_data.text:
            items.append({"href": href_el.text, "ical_text": cal_data.text})
    return items


def put_ical(href: str, ical_text: str, cal_name: Optional[str] = None) -> bool:
    """
    PUT un ítem iCal. Devuelve True si OK.

    Radicale a veces devuelve hrefs con el UUID interno del calendario
    (ej: /usuario/a1b2c3-uuid-del-cal/item.ics) en lugar de la ruta
    nombrada (ej: /usuario/mi-calendario/item.ics). Usar esa ruta interna
    en el PUT puede dar 403 aunque el acceso esté permitido sobre la ruta
    nombrada.

    Si se pasa `cal_name`, se reconstruye la URL usando ese nombre de
    calendario más el nombre de fichero del href original, garantizando
    que el PUT vaya a la ruta con permisos.
    """
    if cal_name:
        filename = os.path.basename(href.rstrip("/"))
        href = f"{RADICALE_BASE.rstrip('/')}/{cal_name}/{filename}"

    url = href if href.startswith("http") else RADICALE_URL + href
    headers = {"Content-Type": "text/calendar; charset=utf-8"}
    r = requests.put(
        url,
        data=ical_text.encode("utf-8"),
        headers=headers,
        auth=(RADICALE_USER, RADICALE_PW),
        timeout=30,
    )
    if r.status_code not in (200, 201, 204):
        print(f"    ⚠️  PUT {href} → HTTP {r.status_code}: {r.text[:120]}")
        return False
    return True


def _find_vtodo(cal: Calendar):
    """
    Localiza el componente VTODO de un Calendar para mutarlo in place.

    IMPORTANTE: no reconstruir el Calendar iterando cal.walk() y volviendo a
    añadir cada componente a un Calendar() nuevo -- walk() aplana también los
    subcomponentes anidados (p.ej. VALARM, que DAVx5/Tasks.org añade siempre
    a sus VTODO), así que ese patrón los duplica como hermanos de nivel
    superior en vez de dejarlos anidados dentro del VTODO. Eso produce iCal
    inválido y Radicale lo rechaza con HTTP 400 -- son tareas reales del
    usuario, no basura, así que este bug las dejaba siempre desactualizadas.
    Mutar el componente encontrado aquí y llamar a cal.to_ical() directamente
    conserva la estructura anidada intacta.
    """
    for comp in cal.walk():
        if hasattr(comp, "name") and comp.name == "VTODO":
            return comp
    return None


# ─────────────────────────────────────────────────────────────────────────────
#  PARSEO DE ÍTEMS iCAL
# ─────────────────────────────────────────────────────────────────────────────

def parse_events(raw_items: list[dict], since_date: date) -> dict:
    """
    Extrae VEVENTs cuya DTSTART >= since_date.

    Retorna dict keyed por (artist_norm, album_norm):
        {artist, album, release_date (iso), href, uid, ical_text}
    """
    events: dict = {}
    for item in raw_items:
        try:
            cal = Calendar.from_ical(item["ical_text"])
        except Exception as e:
            print(f"  ⚠️  Error parseando ítem: {e}")
            continue
        for comp in cal.walk():
            if not hasattr(comp, "name") or comp.name != "VEVENT":
                continue
            summary = str(comp.get("SUMMARY", ""))
            if not summary:
                continue
            artist, album = parse_summary(summary)
            if not album:
                continue
            dt_start = parse_date_value(comp.get("DTSTART"))
            if dt_start is None or dt_start < since_date:
                continue
            key = (_normalize(artist), _normalize(album))
            events[key] = {
                "artist":       artist,
                "album":        album,
                "release_date": dt_start.isoformat(),
                "href":         item["href"],
                "uid":          str(comp.get("UID", "")),
                "ical_text":    item["ical_text"],
            }
    return events


def parse_tasks(raw_items: list[dict]) -> dict:
    """
    Extrae VTODOs.

    Retorna dict keyed por (artist_norm, album_norm):
        {artist, album, listened_date, completed, href, uid, ical_text}
    """
    tasks: dict = {}
    for item in raw_items:
        try:
            cal = Calendar.from_ical(item["ical_text"])
        except Exception as e:
            print(f"  ⚠️  Error parseando tarea: {e}")
            continue
        for comp in cal.walk():
            if not hasattr(comp, "name") or comp.name != "VTODO":
                continue
            summary = str(comp.get("SUMMARY", ""))
            if not summary:
                continue
            artist, album = parse_summary(summary)
            if not album:
                continue

            completed = parse_date_value(comp.get("COMPLETED"))
            status    = str(comp.get("STATUS", "")).upper()

            key = (_normalize(artist), _normalize(album))
            tasks[key] = {
                "artist":          artist,
                "album":           album,
                "listened_date":   completed.isoformat() if completed else None,
                "completed":       completed is not None or status == "COMPLETED",
                "href":            item["href"],
                "uid":             str(comp.get("UID", "")),
                "ical_text":       item["ical_text"],
            }
    return tasks


def update_vtodo_completed(task: dict, listened_date: date) -> bool:
    try:
        cal = Calendar.from_ical(task["ical_text"])
    except Exception as e:
        print(f"    ⚠️  Error parseando VTODO: {e}")
        return False

    comp = _find_vtodo(cal)
    if comp is None:
        print("    ⚠️  No se encontró el VTODO en el iCal")
        return False

    comp["STATUS"] = vText("COMPLETED")
    listened_dt = datetime.combine(
        listened_date, datetime.min.time(), tzinfo=timezone.utc)
    if "COMPLETED" not in comp:
        comp.add("COMPLETED", listened_dt)
    else:
        comp["COMPLETED"] = vDatetime(listened_dt)
    comp["LAST-MODIFIED"] = vDatetime(datetime.now(tz=timezone.utc))

    return put_ical(task["href"], cal.to_ical().decode("utf-8"), cal_name=CALENDAR_TASKS)


# ─────────────────────────────────────────────────────────────────────────────
#  MUSICBRAINZ
# ─────────────────────────────────────────────────────────────────────────────

_mb_last_call: float = 0.0
_LUCENE_SPECIAL = re.compile(r'([\+\-\!\(\)\{\}\[\]\^"~\*\?:\\\/])')


def _mb_escape(s: str) -> str:
    return _LUCENE_SPECIAL.sub(r"\\\1", s)


def mb_get(endpoint: str, params: dict, _attempt: int = 0) -> Optional[dict]:
    global _mb_last_call, _mb_session

    MAX_RETRIES = 5
    elapsed = time.time() - _mb_last_call
    if elapsed < MB_RATE_LIMIT:
        time.sleep(MB_RATE_LIMIT - elapsed)

    try:
        r = _mb_session.get(
            MB_BASE + endpoint,
            params={**params, "fmt": "json"},
            timeout=60,
        )
    except (requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.SSLError,
            requests.exceptions.ChunkedEncodingError) as exc:
        _mb_last_call = time.time()
        if _attempt >= MAX_RETRIES:
            print(f"\n    ⚠️  MB: error de red tras {MAX_RETRIES} reintentos ({exc.__class__.__name__})")
            return None
        wait = 5 * (2 ** _attempt)
        print(f"\n    MB: reintento {_attempt+1}/{MAX_RETRIES} en {wait}s...", end="", flush=True)
        if isinstance(exc, requests.exceptions.SSLError):
            _mb_session.close()
            _mb_session = requests.Session()
            _mb_session.headers.update({"User-Agent": MB_UA})
            _mb_session.verify = _MB_VERIFY
        time.sleep(wait)
        return mb_get(endpoint, params, _attempt=_attempt + 1)

    _mb_last_call = time.time()

    if r.status_code == 400:
        print(f"\n    ⚠️  MB: HTTP 400 — query inválida, se omite")
        return None
    if r.status_code == 404:
        return None
    if r.status_code in (429, 503):
        wait = max(int(r.headers.get("Retry-After", 10 * (2 ** _attempt))), 10)
        if _attempt >= MAX_RETRIES:
            print(f"\n    ⚠️  MB: HTTP {r.status_code} persistente, se omite")
            return None
        print(f"\n    MB: rate-limit {r.status_code}, esperando {wait}s...", end="", flush=True)
        time.sleep(wait)
        return mb_get(endpoint, params, _attempt=_attempt + 1)
    if r.status_code in (500, 502, 504):
        wait = 5 * (2 ** _attempt)
        if _attempt >= MAX_RETRIES:
            print(f"\n    ⚠️  MB: HTTP {r.status_code} persistente, se omite")
            return None
        print(f"\n    MB: error {r.status_code}, reintento {_attempt+1} en {wait}s...", end="", flush=True)
        time.sleep(wait)
        return mb_get(endpoint, params, _attempt=_attempt + 1)

    r.raise_for_status()
    return r.json()


def _pick_best_release(releases: list[dict], artist: str) -> Optional[dict]:
    """
    De una lista de releases MB, elige el más relevante:
    primero score=100 del artista correcto, luego cualquier score=100,
    luego el primero con artista correcto, luego el primero.
    """
    artist_n = _normalize(artist)

    def matches_artist(r: dict) -> bool:
        return any(
            _normalize(ac.get("artist", {}).get("name", "")) == artist_n
            for ac in r.get("artist-credit", [])
            if isinstance(ac, dict)
        )

    for candidate in [
        next((r for r in releases if str(r.get("score", 0)) == "100" and matches_artist(r)), None),
        next((r for r in releases if str(r.get("score", 0)) == "100"), None),
        next((r for r in releases if matches_artist(r)), None),
        releases[0] if releases else None,
    ]:
        if candidate:
            return candidate
    return None


def get_tracklist(artist: str, album: str) -> list[str]:
    """Retorna lista de títulos normalizados de las pistas del álbum, o []."""
    a_q = _mb_escape(artist)
    b_q = _mb_escape(album)

    releases = None
    for query in [
        f'artist:"{a_q}" AND release:"{b_q}"',
        f'release:"{b_q}" AND artist:"{a_q}"',
        f'release:"{b_q}"',
    ]:
        data = mb_get("release", {"query": query, "limit": 5})
        if data and data.get("releases"):
            releases = data["releases"]
            break

    if not releases:
        print(f"    ℹ️  MusicBrainz: no encontrado '{artist} — {album}'")
        return []

    best = _pick_best_release(releases, artist)
    if not best:
        return []
    mbid = best.get("id")
    if not mbid:
        return []

    detail = mb_get(f"release/{mbid}", {"inc": "recordings"})
    if not detail:
        return []

    tracks = []
    for medium in detail.get("media", []):
        for track in medium.get("tracks", []):
            title = track.get("title") or (track.get("recording") or {}).get("title", "")
            if title:
                tracks.append(_normalize(title))

    print(f"    🎵 MusicBrainz: {len(tracks)} pistas para '{artist} — {album}'")
    return tracks


def _complete_partial_date(raw: str) -> str:
    """
    Completa fechas parciales de MusicBrainz:
      YYYY       → YYYY-12-31  (fin de año, no comienzo)
      YYYY-MM    → YYYY-MM-<último día del mes>
      YYYY-MM-DD → sin cambios
    """
    import calendar as _cal
    parts = raw.split("-")
    if len(parts) == 1:
        return f"{parts[0]}-12-31"
    if len(parts) == 2:
        try:
            year, month = int(parts[0]), int(parts[1])
            last_day = _cal.monthrange(year, month)[1]
            return f"{parts[0]}-{parts[1]}-{last_day:02d}"
        except ValueError:
            return raw
    return raw


def get_release_date_from_mb(artist: str, album: str) -> Optional[str]:
    """
    Busca la fecha de lanzamiento en MusicBrainz.
    Estrategia (orden de prioridad):
      1. release-group con artist + releasegroup → first-release-date (fecha canónica)
      2. release con artist + release (varias variantes de query)
    Fechas parciales: YYYY → YYYY-12-31, YYYY-MM → YYYY-MM-<último día del mes>.
    """
    a_q = _mb_escape(artist)
    b_q = _mb_escape(album)

    # 1. release-group: first-release-date es la fecha más fiable y canónica
    for rg_query in [
        f'artist:"{a_q}" AND releasegroup:"{b_q}"',
        f'releasegroup:"{b_q}" AND artist:"{a_q}"',
    ]:
        data = mb_get("release-group", {"query": rg_query, "limit": 5})
        if not data or not data.get("release-groups"):
            continue
        rgs = data["release-groups"]
        best = next((r for r in rgs if str(r.get("score", 0)) == "100"), rgs[0])
        raw = best.get("first-release-date", "").strip()
        if raw:
            return _complete_partial_date(raw)

    # 2. release: fallback con varias variantes de búsqueda
    for rel_query in [
        f'artist:"{a_q}" AND release:"{b_q}"',
        f'release:"{b_q}" AND artist:"{a_q}"',
        f'release:"{b_q}"',
    ]:
        data = mb_get("release", {"query": rel_query, "limit": 5})
        if not data or not data.get("releases"):
            continue
        releases = data["releases"]
        # Si la query no filtra por artista, descartar resultados de otro artista
        if f'artist:' not in rel_query:
            releases = [
                r for r in releases
                if any(
                    _normalize(ac.get("artist", {}).get("name", "")) == _normalize(artist)
                    for ac in r.get("artist-credit", [])
                    if isinstance(ac, dict)
                )
            ] or releases  # si no queda nada, usar todos
        best = next((r for r in releases if str(r.get("score", 0)) == "100"), releases[0])
        raw = best.get("date", "").strip()
        if raw:
            return _complete_partial_date(raw)

    return None


# ─────────────────────────────────────────────────────────────────────────────
#  LASTFM DB
# ─────────────────────────────────────────────────────────────────────────────

def find_first_listen(lastfm_conn: sqlite3.Connection,
                      artist: str, tracks: list[str],
                      min_date: Optional[date] = None) -> Optional[date]:
    if not tracks:
        return None

    artist_key = _normalize(artist)
    row = lastfm_conn.execute(
        "SELECT artist_id FROM artists WHERE name_normalized = ?", (artist_key,)
    ).fetchone()
    if not row:
        row = lastfm_conn.execute(
            "SELECT artist_id FROM artists WHERE name_normalized LIKE ?",
            (f"%{artist_key}%",)
        ).fetchone()
    if not row:
        print(f"    ℹ️  Last.fm DB: '{artist}' no encontrado")
        return None

    artist_id = row[0]
    placeholders = ",".join("?" * len(tracks))
    result = lastfm_conn.execute(
        f"SELECT MIN(ts), MIN(ts_iso) FROM scrobbles "
        f"WHERE artist_id = ? AND track_normalized IN ({placeholders})",
        [artist_id, *tracks],
    ).fetchone()

    if result and result[0]:
        try:
            return datetime.fromisoformat(result[1]).date()
        except Exception:
            return datetime.fromtimestamp(result[0], tz=timezone.utc).date()

    # Búsqueda fuzzy por la primera palabra significativa de cada pista
    earliest: Optional[date] = None
    for track in tracks[:10]:
        words = [w for w in track.split() if len(w) > 3]
        if not words:
            continue
        res = lastfm_conn.execute(
            "SELECT MIN(ts), MIN(ts_iso) FROM scrobbles "
            "WHERE artist_id = ? AND track_normalized LIKE ?",
            (artist_id, f"%{words[0]}%"),
        ).fetchone()
        if res and res[0]:
            try:
                d = datetime.fromisoformat(res[1]).date()
            except Exception:
                d = datetime.fromtimestamp(res[0], tz=timezone.utc).date()
            if earliest is None or d < earliest:
                earliest = d


    if result and result[0]:
        try:
            found = datetime.fromisoformat(result[1]).date()
        except Exception:
            found = datetime.fromtimestamp(result[0], tz=timezone.utc).date()
        if min_date and found < min_date:
            print(f"    ⚠️  Scrobble ignorado ({found}) anterior al lanzamiento ({min_date})")
            return None          # ← descarta falso positivo
        return found

    if min_date and earliest and earliest < min_date:
        print(f"    ⚠️  Scrobble fuzzy ignorado ({earliest}) anterior al lanzamiento ({min_date})")
        return None
    return earliest


# ─────────────────────────────────────────────────────────────────────────────
#  MUSIC_STATS DB
# ─────────────────────────────────────────────────────────────────────────────

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


def init_db(conn: sqlite3.Connection):
    conn.executescript(SCHEMA)
    # Migraciones aditivas sobre DBs ya desplegadas -- CREATE TABLE IF NOT
    # EXISTS no toca una tabla existente. Las columnas de compra/tienda
    # (purchase_date, purchase_date_estimated, days_release_to_purchase,
    # days_purchase_to_listened) de versiones anteriores se dejan huérfanas
    # en vez de borrarlas: DROP COLUMN es más arriesgado que no tocarlas, y
    # no molestan sin usarlas.
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


def _sanitize_chain(release_date:  Optional[str],
                    listened_date: Optional[str]) -> tuple[Optional[str], Optional[str]]:
    """
    Garantiza que release ≤ listened. Una escucha anterior al lanzamiento
    se descarta (se deja None) -- evita que reediciones/remasters con fecha
    reciente den un days_release_to_listened negativo por un falso positivo
    de Last.fm (p.ej. un scrobble de una edición anterior mal emparejado).
    """
    if not release_date or not listened_date:
        return release_date, listened_date
    try:
        if date.fromisoformat(listened_date) < date.fromisoformat(release_date):
            return release_date, None
    except ValueError:
        pass
    return release_date, listened_date


def upsert_album(conn: sqlite3.Connection,
                 artist: str, album: str,
                 release_date:  Optional[str],
                 listened_date: Optional[str]):
    """Inserta o actualiza el álbum en music_stats.db."""
    release_date, listened_date = _sanitize_chain(release_date, listened_date)
    if listened_date is None and release_date is None:
        return  # nada útil que guardar

    artist_key = _normalize(artist)
    album_key  = _normalize(album)

    row = conn.execute(
        "SELECT artist_id FROM artists WHERE name_normalized = ?", (artist_key,)
    ).fetchone()
    if row:
        artist_id = row[0]
    else:
        artist_id = conn.execute(
            "INSERT INTO artists (name, name_normalized) VALUES (?, ?)",
            (artist, artist_key)
        ).lastrowid

    existing = conn.execute(
        """SELECT album_id, release_date, listened_date
           FROM albums WHERE artist_id = ? AND name_normalized = ?""",
        (artist_id, album_key)
    ).fetchone()

    if existing is None:
        conn.execute(
            """INSERT INTO albums
               (artist_id, name, name_normalized,
                release_date, listened_date, days_release_to_listened)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                artist_id, album, album_key,
                release_date, listened_date,
                days_between(release_date, listened_date),
            )
        )
    else:
        al_id, old_rel, old_lis = existing
        new_rel = release_date  or old_rel
        new_lis = listened_date or old_lis
        if (new_rel, new_lis) != (old_rel, old_lis):
            conn.execute(
                """UPDATE albums SET
                   release_date              = ?,
                   listened_date             = ?,
                   days_release_to_listened  = ?
                   WHERE album_id = ?""",
                (
                    new_rel, new_lis,
                    days_between(new_rel, new_lis),
                    al_id,
                )
            )

def get_release_from_db(conn: sqlite3.Connection, artist: str, album: str) -> Optional[str]:
    """Consulta si el álbum ya tiene una fecha de lanzamiento registrada en la DB."""
    artist_key = _normalize(artist)
    album_key  = _normalize(album)
    row = conn.execute(
        """SELECT release_date FROM albums a
           JOIN artists art ON a.artist_id = art.artist_id
           WHERE art.name_normalized = ? AND a.name_normalized = ?""",
        (artist_key, album_key)
    ).fetchone()
    return row[0] if row and row[0] else None

# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sincroniza lanzamientos del calendario con tareas, "
                    "Last.fm y la base de datos. "
                    "Fuente de verdad: VTODOs del calendario de tareas."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Solo muestra qué haría, sin escribir nada"
    )
    parser.add_argument(
        "--auto", action="store_true",
        help="Modo no interactivo: omite álbumes sin fecha de lanzamiento (no pregunta al usuario)"
    )
    args = parser.parse_args()

    print(f"🎵 sync_music.py{' [DRY RUN]' if args.dry_run else ''}")
    print("=" * 60)

    # Validaciones
    missing = [v for v in ("RADICALE_URL", "RADICALE_USERNAME", "CALENDAR_NAME", "CALENDAR_TASKS")
               if not os.getenv(v)]
    if missing:
        print(f"❌ Variables de entorno faltantes en .env: {', '.join(missing)}")
        sys.exit(1)

    # ── 1. Descargar calendarios ──────────────────────────────────────────────
    print(f"\n📋 Leyendo calendario de tareas ({CALENDAR_TASKS})...")
    try:
        raw_tasks = fetch_calendar_items(CALENDAR_TASKS)
        print(f"   {len(raw_tasks)} ítems descargados")
    except Exception as e:
        print(f"  ❌ Error CalDAV (tareas): {e}")
        sys.exit(1)

    print(f"\n📅 Leyendo calendario de lanzamientos ({CALENDAR_NAME})...")
    try:
        raw_events = fetch_calendar_items(CALENDAR_NAME)
        print(f"   {len(raw_events)} ítems descargados")
    except Exception as e:
        print(f"  ❌ Error CalDAV (eventos): {e}")
        sys.exit(1)

    # ── 2. Parsear ────────────────────────────────────────────────────────────
    print("\n🔍 Clasificando VEVENTs y VTODOs...")
    events_all = parse_events(raw_events, date.min)   # sin filtro de fecha
    tasks = parse_tasks(raw_tasks)
    print(f"   VEVENTs total:      {len(events_all)}")
    print(f"   VTODOs total:       {len(tasks)}")

    # ── 3. Abrir DBs ──────────────────────────────────────────────────────────
    stats = {"listened_updated": 0, "already_ok": 0,
             "no_listen": 0, "db_updated": 0}
    lastfm_conn: Optional[sqlite3.Connection] = None
    if os.path.exists(LASTFM_DB):
        # Solo lectura: lastfm_data se monta :ro en este servicio (lo rellena
        # lastfm-scrobbles a diario). mode=ro no basta -- la DB suele estar en
        # journal_mode=WAL (la pone el escritor), y hasta un lector necesita
        # abrir/crear el -shm junto a la DB para eso, lo que revienta con
        # "unable to open database file" en un volumen de solo lectura.
        # immutable=1 le dice a SQLite que confíe en que el archivo no
        # cambiará durante la conexión y se salte esa comprobación.
        lastfm_conn = sqlite3.connect(f"file:{LASTFM_DB}?mode=ro&immutable=1", uri=True)
        print(f"\n💾 Last.fm DB: {LASTFM_DB}")
    else:
        print(f"\n⚠️  Last.fm DB no encontrada en {LASTFM_DB!r} — se omitirá fecha de escucha")

    music_conn = sqlite3.connect(MUSIC_DB)
    music_conn.execute("PRAGMA foreign_keys=ON")
    music_conn.execute("PRAGMA journal_mode=WAL")
    init_db(music_conn)

    # ── 4. Loop principal: un VTODO a la vez ─────────────────────────────────
    print(f"\n⚙️  Procesando {len(tasks)} VTODO(s) como fuente de verdad...")
    for key, task in tasks.items():
        artist        = task["artist"]
        album         = task["album"]
        listened_date = task.get("listened_date")   # COMPLETED del VTODO
        is_completed  = task.get("completed", False)

        # release_date: del VEVENT cruzado por nombre (puede ser None)
        ev           = events_all.get(key)
        release_date = ev["release_date"] if ev else None

        # Si no está en el calendario, miramos si ya lo guardamos en nuestra DB antes
        if release_date is None:
            release_date = get_release_from_db(music_conn, artist, album)
            if release_date:
                print(f"    ℹ️ Fecha recuperada de music_stats.db: {release_date}")

        # Si sigue siendo None, buscamos fuera
        if release_date is None:
            print(f"\n  🎸 {artist} — {album}  (sin VEVENT ni registro previo, buscando en MusicBrainz...)")
            release_date = get_release_date_from_mb(artist, album)
            if release_date:
                print(f"    📅 MusicBrainz: lanzamiento el {release_date}")
            elif args.auto:
                print(f"    ❓ No encontrado en MusicBrainz — omitido (--auto)")
                continue
            else:
                print(f"    ❓ No encontrado en MusicBrainz")
                user_input = input(
                    f"    Fecha de lanzamiento para '{artist} — {album}'"
                    f" (YYYY-MM-DD, vacío para omitir): "
                ).strip()
                release_date = user_input or None

        print(f"\n  🎸 {artist} — {album}"
              f"  (release={release_date or '?'}"
              f"  listened={listened_date or '?'})")

        # ── 4a. Actualizar DB con lo que tenemos del VTODO ───────────────────
        if not args.dry_run:
            upsert_album(music_conn, artist, album, release_date, listened_date)
            music_conn.commit()
        stats["db_updated"] += 1

        # ── 4b. Si el VTODO ya está completado → no consultar Last.fm ────────
        if is_completed:
            print(f"    ✔️  Ya completado: {listened_date}")
            stats["already_ok"] += 1
            continue

        # ── 4c. Buscar primera escucha en Last.fm ─────────────────────────────
        if lastfm_conn is None:
            stats["no_listen"] += 1
            continue

        tracks = get_tracklist(artist, album)
        if not tracks:
            stats["no_listen"] += 1
            continue

        min_date = date.fromisoformat(release_date) if release_date else None
        first_listen = find_first_listen(lastfm_conn, artist, tracks, min_date=min_date)

        if first_listen is None:
            print(f"    ℹ️  Sin escuchas en Last.fm todavía")
            stats["no_listen"] += 1
            continue

        print(f"    🎧 Primera escucha: {first_listen.isoformat()}")

        if not args.dry_run and task.get("ical_text"):
            ok = update_vtodo_completed(task, first_listen)
            if ok:
                print(f"    ✅ VTODO marcado COMPLETED")
                stats["listened_updated"] += 1
                upsert_album(music_conn, artist, album,
                             release_date, first_listen.isoformat())
                music_conn.commit()
        elif args.dry_run:
            print(f"    [DRY RUN] pondría COMPLETED={first_listen.isoformat()}")
            stats["listened_updated"] += 1

    # ── 5. Resumen ────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📊 Resumen:")
    print(f"   VTODOs procesados:         {stats['db_updated']}")
    print(f"   Fechas de escucha nuevas:  {stats['listened_updated']}")
    print(f"   Ya completados:            {stats['already_ok']}")
    print(f"   Sin escucha en Last.fm:    {stats['no_listen']}")

    if lastfm_conn:
        lastfm_conn.close()
    music_conn.close()
    print("\n✅ ¡Hecho!")


if __name__ == "__main__":
    main()
