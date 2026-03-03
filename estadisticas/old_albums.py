#!/usr/bin/env python3
"""
sync_calendar.py — Sincroniza eventos de lanzamiento (VEVENT) con tareas (VTODO)
en Radicale y rellena las fechas de escucha desde lastfm_stats.db.

Uso:
    python sync_calendar.py --since 365   # últimos 365 días
    python sync_calendar.py --since 30    # último mes

Variables en .env:
    RADICALE_URL        — URL base de Radicale  (ej: http://localhost:5232)
    RADICALE_USERNAME   — usuario de Radicale
    RADICALE_PW         — contraseña de Radicale
    RADICALE_CALENDAR   — ruta del calendario    (ej: /usuario/musica/)
    LASTFM_DB           — ruta a lastfm_stats.db (por defecto: lastfm_stats.db)
    MUSIC_DB            — ruta a music_stats.db  (por defecto: music_stats.db)
    STORE_CSV           — ruta a albums.csv      (por defecto: albums.csv)
    MB_EMAIL            — email para User-Agent de MusicBrainz

Lógica:
    1. Lee VEVENTs del calendario en el rango --since.
    2. Para cada evento, busca si existe un VTODO con el mismo SUMMARY.
    3. Si no existe VTODO → lo crea en Radicale con DTSTART = release_date
       y añade la línea al CSV.
    4. Consulta MusicBrainz para obtener el tracklist del álbum.
    5. Busca en lastfm_stats.db la primera vez que se escuchó alguna de esas
       canciones con ese artista → fecha de escucha.
    6. Si hay fecha de escucha y el VTODO no tiene COMPLETED → actualiza en
       Radicale (COMPLETED + STATUS:COMPLETED) y en music_stats.db.
"""

import argparse
import csv
import os
import re
import sqlite3
import time
import uuid
from datetime import datetime, date, timezone, timedelta
from typing import Optional
from xml.etree import ElementTree as ET

import requests
from dotenv import load_dotenv
from icalendar import Calendar, Event, Todo, vDatetime, vDate, vText

# Intentar usar certifi para certificados actualizados.
# Si no está instalado, requests usará los del sistema.
try:
    import certifi
    _MB_VERIFY = certifi.where()
except ImportError:
    _MB_VERIFY = True

load_dotenv()

# ─────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────
RADICALE_URL      = os.getenv("RADICALE_URL", "").rstrip("/")
RADICALE_USER     = os.getenv("RADICALE_USERNAME", "")
RADICALE_PW       = os.getenv("RADICALE_PW", "")
RADICALE_CALENDAR = os.getenv("RADICALE_CALENDAR", "/")   # ej: /user/music/

LASTFM_DB  = os.getenv("LASTFM_DB",  "lastfm_stats.db")
MUSIC_DB   = os.getenv("MUSIC_DB",   "music_stats.db")
STORE_CSV  = os.getenv("STORE_CSV",  "albums.csv")
MB_EMAIL   = os.getenv("MB_EMAIL",   "user@example.com")

MB_BASE       = "https://musicbrainz.org/ws/2/"
MB_UA         = f"SyncCalendar/1.0 ({MB_EMAIL})"
MB_RATE_LIMIT = 1.5   # segundos entre llamadas a MB (MusicBrainz: max 1 req/s)

# Sesión persistente para MusicBrainz: reutiliza la conexión TCP/SSL
# entre llamadas, evitando renegociaciones SSL que fallan en algunos entornos.
_mb_session = requests.Session()
_mb_session.headers.update({"User-Agent": MB_UA})
_mb_session.verify = _MB_VERIFY

# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def parse_date_value(dt_val) -> Optional[date]:
    """Convierte un valor icalendar a date."""
    if dt_val is None:
        return None
    if hasattr(dt_val, "dt"):
        dt_val = dt_val.dt
    if isinstance(dt_val, datetime):
        return dt_val.date()
    if isinstance(dt_val, date):
        return dt_val
    return None


def strip_emojis(s: str) -> str:
    return re.sub(
        r"^[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+"
        r"|[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+$",
        "", s,
    ).strip()


def parse_summary(summary: str) -> tuple[str, str]:
    """'Artist - Album' → (artist, album). Tolera —, –, -."""
    summary = strip_emojis(summary)
    parts = re.split(r"\s+[-–—]\s+", summary, maxsplit=1)
    if len(parts) == 2:
        return strip_emojis(parts[0]), strip_emojis(parts[1])
    return summary, ""


def days_between(d1: Optional[str], d2: Optional[str]) -> Optional[int]:
    if not d1 or not d2:
        return None
    try:
        return (date.fromisoformat(d2) - date.fromisoformat(d1)).days
    except ValueError:
        return None


# ─────────────────────────────────────────────
#  CALDAV
# ─────────────────────────────────────────────

def caldav_url() -> str:
    return RADICALE_URL + RADICALE_CALENDAR


def fetch_calendar_items() -> list[dict]:
    """
    Usa REPORT para obtener todos los ítems del calendario.
    Devuelve lista de dicts: {href, ical_text}.
    """
    url = caldav_url()
    body = """<?xml version="1.0" encoding="UTF-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <D:getetag/>
    <C:calendar-data/>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR"/>
  </C:filter>
</C:calendar-query>"""

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
        href_el    = resp.find("D:href", ns)
        cal_data   = resp.find(".//C:calendar-data", ns)
        if href_el is not None and cal_data is not None and cal_data.text:
            items.append({"href": href_el.text, "ical_text": cal_data.text})
    return items


def put_ical(href: str, ical_text: str) -> bool:
    """PUT un ítem al calendario. Devuelve True si OK."""
    # href puede ser relativo; construimos URL absoluta si es necesario
    if href.startswith("http"):
        url = href
    else:
        url = RADICALE_URL + href

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


# ─────────────────────────────────────────────
#  PARSE ICAL ITEMS
# ─────────────────────────────────────────────

def parse_items(raw_items: list[dict], since_date: date) -> tuple[dict, dict]:
    """
    Clasifica los ítems en VEVENTs y VTODOs.

    events: {key → {"artist", "album", "release_date", "href", "uid", "ical_text"}}
    tasks:  {key → {"artist", "album", "purchase_date", "listened_date",
                    "href", "uid", "ical_text", "completed"}}

    key = (artist_normalized, album_normalized)
    Solo incluye VEVENTs cuya DTSTART >= since_date.
    """
    events: dict = {}
    tasks:  dict = {}

    for item in raw_items:
        try:
            cal = Calendar.from_ical(item["ical_text"])
        except Exception as e:
            print(f"  ⚠️  Error parseando ítem: {e}")
            continue

        for comp in cal.walk():
            comp_name = comp.name if hasattr(comp, "name") else ""

            if comp_name == "VEVENT":
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
                uid = str(comp.get("UID", ""))
                events[key] = {
                    "artist":       artist,
                    "album":        album,
                    "release_date": dt_start.isoformat(),
                    "href":         item["href"],
                    "uid":          uid,
                    "ical_text":    item["ical_text"],
                }

            elif comp_name == "VTODO":
                summary = str(comp.get("SUMMARY", ""))
                if not summary:
                    continue
                artist, album = parse_summary(summary)
                if not album:
                    continue

                dt_start   = parse_date_value(comp.get("DTSTART"))
                completed  = parse_date_value(comp.get("COMPLETED"))
                due        = parse_date_value(comp.get("DUE"))
                status     = str(comp.get("STATUS", "")).upper()
                uid        = str(comp.get("UID", ""))

                key = (_normalize(artist), _normalize(album))
                tasks[key] = {
                    "artist":        artist,
                    "album":         album,
                    "purchase_date": (dt_start or due or None) and
                                     (dt_start or due).isoformat(),
                    "listened_date": completed.isoformat() if completed else None,
                    "completed":     completed is not None or status == "COMPLETED",
                    "href":          item["href"],
                    "uid":           uid,
                    "ical_text":     item["ical_text"],
                }

    return events, tasks


# ─────────────────────────────────────────────
#  VTODO CREATION
# ─────────────────────────────────────────────

def make_vtodo_ical(artist: str, album: str, release_date: str,
                    uid: Optional[str] = None) -> tuple[str, str]:
    """
    Genera el texto iCalendar para un nuevo VTODO.
    Devuelve (uid, ical_text).
    """
    if not uid:
        uid = str(uuid.uuid4())

    cal = Calendar()
    cal.add("PRODID", "-//SyncCalendar//sync_calendar.py//ES")
    cal.add("VERSION", "2.0")

    todo = Todo()
    todo.add("UID",     uid)
    todo.add("SUMMARY", f"{artist} - {album}")
    todo.add("DTSTART", date.fromisoformat(release_date))
    todo.add("STATUS",  "NEEDS-ACTION")
    todo.add("DTSTAMP", datetime.now(tz=timezone.utc))
    todo.add("CREATED", datetime.now(tz=timezone.utc))

    cal.add_component(todo)
    return uid, cal.to_ical().decode("utf-8")


def create_vtodo_in_radicale(artist: str, album: str,
                              release_date: str) -> Optional[str]:
    """
    Crea un VTODO en Radicale. Devuelve el href donde se creó, o None.
    """
    uid, ical_text = make_vtodo_ical(artist, album, release_date)
    href = RADICALE_CALENDAR.rstrip("/") + f"/{uid}.ics"
    ok = put_ical(href, ical_text)
    return href if ok else None


def update_vtodo_completed(task: dict, listened_date: date) -> bool:
    """
    Actualiza el VTODO existente añadiendo COMPLETED y STATUS:COMPLETED.
    """
    try:
        cal = Calendar.from_ical(task["ical_text"])
    except Exception as e:
        print(f"    ⚠️  Error parseando VTODO para actualizar: {e}")
        return False

    updated_cal = Calendar()
    # Copiar propiedades del calendario
    for k, v in cal.items():
        updated_cal.add(k, v)

    for comp in cal.walk():
        if comp.name == "VTODO":
            # Modificar STATUS y añadir COMPLETED
            comp["STATUS"] = vText("COMPLETED")
            if "COMPLETED" not in comp:
                comp.add("COMPLETED", datetime.combine(
                    listened_date, datetime.min.time(), tzinfo=timezone.utc))
            else:
                comp["COMPLETED"] = vDatetime(datetime.combine(
                    listened_date, datetime.min.time(), tzinfo=timezone.utc))
            comp["LAST-MODIFIED"] = vDatetime(datetime.now(tz=timezone.utc))
            updated_cal.add_component(comp)
        elif comp.name != "VCALENDAR":
            updated_cal.add_component(comp)

    ical_text = updated_cal.to_ical().decode("utf-8")
    return put_ical(task["href"], ical_text)


# ─────────────────────────────────────────────
#  CSV
# ─────────────────────────────────────────────

def load_csv(path: str) -> list[dict]:
    """Carga albums.csv → lista de dicts con las columnas disponibles."""
    if not os.path.exists(path):
        return []
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def csv_key(row: dict) -> tuple:
    artist = row.get("artist", row.get("artista", ""))
    album  = row.get("album",  row.get("álbum", row.get("album", "")))
    return (_normalize(artist), _normalize(album))


def append_to_csv(path: str, artist: str, album: str, purchase_date: str):
    """
    Añade una línea al CSV si el álbum no existe ya.
    Crea el fichero con cabecera si no existe.
    """
    existing = load_csv(path)
    key = (_normalize(artist), _normalize(album))
    for row in existing:
        if csv_key(row) == key:
            return  # ya existe

    file_exists = os.path.exists(path)
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists or os.path.getsize(path) == 0:
            writer.writerow(["artist", "album", "purchase_date"])
        writer.writerow([artist, album, purchase_date])

    print(f"    📋 CSV: añadido {artist} — {album} ({purchase_date})")


# ─────────────────────────────────────────────
#  MUSICBRAINZ
# ─────────────────────────────────────────────

_mb_last_call = 0.0

# Caracteres especiales de Lucene que MusicBrainz usa en sus queries
_LUCENE_SPECIAL = re.compile(r'([\+\-\!\(\)\{\}\[\]\^"~\*\?:\\\/])')


def _mb_escape(s: str) -> str:
    """Escapa caracteres especiales de Lucene para que MB no devuelva HTTP 400."""
    return _LUCENE_SPECIAL.sub(r'\\\1', s)


def mb_get(endpoint: str, params: dict, _attempt: int = 0) -> Optional[dict]:
    """
    GET a MusicBrainz con rate-limit estricto (1.5 s entre llamadas) y
    reintentos con backoff exponencial ante errores de red, SSL o HTTP 5xx.

    Usa una sesión HTTP persistente (_mb_session) para reutilizar la conexión
    TCP/SSL entre llamadas. Si se producen SSLErrors repetidos, la sesión se
    recrea para forzar un nuevo handshake desde cero.
    """
    global _mb_last_call, _mb_session

    MAX_RETRIES = 5

    # ── Rate-limit ───────────────────────────────────────────────────────────
    elapsed = time.time() - _mb_last_call
    if elapsed < MB_RATE_LIMIT:
        time.sleep(MB_RATE_LIMIT - elapsed)

    query_params = {**params, "fmt": "json"}

    try:
        r = _mb_session.get(
            MB_BASE + endpoint,
            params=query_params,
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
        print(f"\n    MB: error de red ({exc.__class__.__name__}), "
              f"reintento {_attempt+1}/{MAX_RETRIES} en {wait}s...", end="", flush=True)

        # Recrear la sesión en errores SSL para forzar nuevo handshake
        if isinstance(exc, requests.exceptions.SSLError):
            _mb_session.close()
            _mb_session = requests.Session()
            _mb_session.headers.update({"User-Agent": MB_UA})
            _mb_session.verify = _MB_VERIFY

        time.sleep(wait)
        return mb_get(endpoint, params, _attempt=_attempt + 1)

    _mb_last_call = time.time()

    if r.status_code == 400:
        print(f"\n    ⚠️  MB: HTTP 400 (query inválida) — se omite")
        return None

    if r.status_code == 404:
        return None

    if r.status_code in (429, 503):
        wait = int(r.headers.get("Retry-After", 10 * (2 ** _attempt)))
        wait = max(wait, 10)
        if _attempt >= MAX_RETRIES:
            print(f"\n    ⚠️  MB: HTTP {r.status_code} persistente, se omite")
            return None
        print(f"\n    MB: HTTP {r.status_code} (rate-limit), esperando {wait}s...",
              end="", flush=True)
        time.sleep(wait)
        return mb_get(endpoint, params, _attempt=_attempt + 1)

    if r.status_code in (500, 502, 504):
        if _attempt >= MAX_RETRIES:
            print(f"\n    ⚠️  MB: HTTP {r.status_code} persistente, se omite")
            return None
        wait = 5 * (2 ** _attempt)
        print(f"\n    MB: HTTP {r.status_code}, reintento {_attempt+1}/{MAX_RETRIES} "
              f"en {wait}s...", end="", flush=True)
        time.sleep(wait)
        return mb_get(endpoint, params, _attempt=_attempt + 1)

    r.raise_for_status()
    return r.json()


def get_tracklist_from_mb(artist: str, album: str) -> list[str]:
    """
    Busca el álbum en MusicBrainz y devuelve lista de títulos de canciones
    (normalizados). Devuelve lista vacía si no encuentra nada.
    """
    artist_q = _mb_escape(artist)
    album_q  = _mb_escape(album)

    # 1. Buscar el release
    data = mb_get("release", {
        "query": f'artist:"{artist_q}" AND release:"{album_q}"',
        "limit": 3,
    })
    if not data or not data.get("releases"):
        # Segundo intento: solo nombre del álbum
        data = mb_get("release", {
            "query": f'release:"{album_q}" AND artist:"{artist_q}"',
            "limit": 5,
        })

    if not data or not data.get("releases"):
        print(f"    ℹ️  MusicBrainz: no se encontró '{artist} — {album}'")
        return []

    # Elegir el release con más score o el primero
    releases = data["releases"]
    best = releases[0]
    for rel in releases:
        if str(rel.get("score", 0)) == "100":
            best = rel
            break

    mbid = best.get("id")
    if not mbid:
        return []

    # 2. Obtener el tracklist
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


# ─────────────────────────────────────────────
#  LASTFM DB
# ─────────────────────────────────────────────

def find_first_listen(lastfm_conn: sqlite3.Connection,
                      artist: str, tracks: list[str]) -> Optional[date]:
    """
    Busca en lastfm_stats.db la fecha del primer scrobble de alguna de las
    pistas dadas para ese artista.
    Devuelve la fecha (date) más antigua, o None.
    """
    if not tracks:
        return None

    artist_key = _normalize(artist)
    row = lastfm_conn.execute(
        "SELECT artist_id FROM artists WHERE name_normalized = ?", (artist_key,)
    ).fetchone()

    if not row:
        # Búsqueda aproximada: LIKE
        row = lastfm_conn.execute(
            "SELECT artist_id FROM artists WHERE name_normalized LIKE ?",
            (f"%{artist_key}%",)
        ).fetchone()

    if not row:
        print(f"    ℹ️  Last.fm DB: artista '{artist}' no encontrado")
        return None

    artist_id = row[0]

    # Buscar el MIN(ts) para cualquiera de las pistas
    placeholders = ",".join("?" * len(tracks))
    result = lastfm_conn.execute(
        f"""SELECT MIN(ts), MIN(ts_iso)
            FROM scrobbles
            WHERE artist_id = ?
              AND track_normalized IN ({placeholders})""",
        [artist_id, *tracks],
    ).fetchone()

    if result and result[0]:
        # ts_iso es una cadena ISO, extraemos la fecha
        ts_iso = result[1]
        try:
            return datetime.fromisoformat(ts_iso).date()
        except Exception:
            return datetime.fromtimestamp(result[0], tz=timezone.utc).date()

    # Fallback: búsqueda difusa por LIKE en track_normalized
    # (por si hay ligeras diferencias de título)
    earliest = None
    for track in tracks[:10]:   # límite para no saturar
        words = [w for w in track.split() if len(w) > 3]
        if not words:
            continue
        like_pat = "%" + words[0] + "%"
        res = lastfm_conn.execute(
            """SELECT MIN(ts), MIN(ts_iso) FROM scrobbles
               WHERE artist_id = ? AND track_normalized LIKE ?""",
            (artist_id, like_pat),
        ).fetchone()
        if res and res[0]:
            try:
                d = datetime.fromisoformat(res[1]).date()
            except Exception:
                d = datetime.fromtimestamp(res[0], tz=timezone.utc).date()
            if earliest is None or d < earliest:
                earliest = d

    return earliest


# ─────────────────────────────────────────────
#  MUSIC_STATS DB
# ─────────────────────────────────────────────

def _normalize_db(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())


def update_music_db(conn: sqlite3.Connection,
                    artist: str, album: str,
                    release_date:  Optional[str],
                    purchase_date: Optional[str],
                    listened_date: Optional[str]):
    """
    Inserta o actualiza el álbum en music_stats.db.
    """
    artist_key = _normalize_db(artist)
    album_key  = _normalize_db(album)

    # get_or_create artist
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

    # get album
    existing = conn.execute(
        """SELECT album_id, release_date, store_date, purchase_date, listened_date
           FROM albums WHERE artist_id = ? AND name_normalized = ?""",
        (artist_id, album_key)
    ).fetchone()

    if existing is None:
        conn.execute(
            """INSERT INTO albums
               (artist_id, name, name_normalized,
                release_date, purchase_date, listened_date,
                days_release_to_store, days_store_to_purchase, days_purchase_to_listened)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                artist_id, album, album_key,
                release_date, purchase_date, listened_date,
                None,
                days_between(release_date, purchase_date),
                days_between(purchase_date, listened_date),
            )
        )
    else:
        al_id, old_rel, old_store, old_pur, old_lis = existing
        new_rel = release_date  or old_rel
        new_pur = purchase_date or old_pur
        new_lis = listened_date or old_lis
        if (new_rel, new_pur, new_lis) != (old_rel, old_pur, old_lis):
            conn.execute(
                """UPDATE albums SET
                   release_date              = ?,
                   purchase_date             = ?,
                   listened_date             = ?,
                   days_store_to_purchase    = ?,
                   days_purchase_to_listened = ?
                   WHERE album_id = ?""",
                (
                    new_rel, new_pur, new_lis,
                    days_between(new_rel, new_pur),
                    days_between(new_pur, new_lis),
                    al_id,
                )
            )


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Sincroniza eventos de lanzamiento con tareas en Radicale "
                    "y fechas de escucha desde Last.fm."
    )
    parser.add_argument(
        "--since", type=int, default=90, metavar="DÍAS",
        help="Número de días hacia atrás desde hoy a consultar (defecto: 90)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="No escribe nada en Radicale ni en ficheros; solo muestra qué haría"
    )
    args = parser.parse_args()

    since_date = date.today() - timedelta(days=args.since)
    print(f"🎵 sync_calendar.py — desde {since_date.isoformat()}"
          f"{' [DRY RUN]' if args.dry_run else ''}")
    print("=" * 60)

    # Validaciones básicas
    if not RADICALE_URL or not RADICALE_USER:
        print("❌ Configura RADICALE_URL, RADICALE_USERNAME y RADICALE_PW en .env")
        return

    # ── 1. Obtener ítems del calendario ──────────────────────────────────────
    print("\n📅 Leyendo calendario de Radicale...")
    try:
        raw_items = fetch_calendar_items()
        print(f"  {len(raw_items)} ítems descargados")
    except Exception as e:
        print(f"  ❌ Error CalDAV: {e}")
        return

    # ── 2. Parsear ────────────────────────────────────────────────────────────
    print("\n🔍 Clasificando VEVENTs y VTODOs...")
    events, tasks = parse_items(raw_items, since_date)
    print(f"  VEVENTs en rango: {len(events)}")
    print(f"  VTODOs total:     {len(tasks)}")

    # ── 3. Abrir DBs ──────────────────────────────────────────────────────────
    lastfm_conn = None
    if os.path.exists(LASTFM_DB):
        lastfm_conn = sqlite3.connect(LASTFM_DB)
        lastfm_conn.execute("PRAGMA journal_mode=WAL")
        print(f"\n💾 Last.fm DB: {LASTFM_DB}")
    else:
        print(f"\n⚠️  Last.fm DB no encontrada en {LASTFM_DB!r} — se omitirá fecha de escucha")

    music_conn = sqlite3.connect(MUSIC_DB)
    music_conn.execute("PRAGMA foreign_keys=ON")
    music_conn.execute("PRAGMA journal_mode=WAL")

    # ── 4. Procesar cada evento ───────────────────────────────────────────────
    print(f"\n⚙️  Procesando {len(events)} eventos...")
    stats = {"vtodo_created": 0, "listened_updated": 0, "already_ok": 0, "no_listen": 0}

    for key, ev in events.items():
        artist       = ev["artist"]
        album        = ev["album"]
        release_date = ev["release_date"]
        task         = tasks.get(key)

        print(f"\n  🎸 {artist} — {album}  ({release_date})")

        # ── 4a. Crear VTODO si no existe ─────────────────────────────────────
        if task is None:
            print(f"    ➕ No existe VTODO → creando (DTSTART={release_date})")
            if not args.dry_run:
                href = create_vtodo_in_radicale(artist, album, release_date)
                if href:
                    print(f"    ✅ VTODO creado en {href}")
                    # Actualizar tasks localmente para el paso siguiente
                    task = {
                        "artist":        artist,
                        "album":         album,
                        "purchase_date": release_date,
                        "listened_date": None,
                        "completed":     False,
                        "href":          href,
                        "uid":           "",
                        "ical_text":     "",
                    }
                    tasks[key] = task
                    stats["vtodo_created"] += 1
                    # Añadir al CSV
                    append_to_csv(STORE_CSV, artist, album, release_date)
                    # Guardar en music_stats.db
                    update_music_db(music_conn, artist, album,
                                    release_date, release_date, None)
                    music_conn.commit()
                else:
                    print("    ❌ Error creando VTODO, se salta")
                    continue
            else:
                print(f"    [DRY RUN] crearía VTODO + CSV + DB para {artist} — {album}")
                stats["vtodo_created"] += 1
        else:
            purchase = task.get("purchase_date") or release_date
            update_music_db(music_conn, artist, album,
                            release_date, purchase, task.get("listened_date"))
            music_conn.commit()

        # ── 4b. Fecha de escucha ──────────────────────────────────────────────
        if task and task.get("completed"):
            print(f"    ✔️  Ya tiene fecha de escucha: {task.get('listened_date')}")
            stats["already_ok"] += 1
            continue

        if lastfm_conn is None:
            stats["no_listen"] += 1
            continue

        # Buscar tracklist en MusicBrainz
        tracks = get_tracklist_from_mb(artist, album)

        if not tracks:
            stats["no_listen"] += 1
            continue

        # Buscar primera escucha en Last.fm DB
        first_listen = find_first_listen(lastfm_conn, artist, tracks)

        if first_listen is None:
            print(f"    ℹ️  Aún no escuchado en Last.fm")
            stats["no_listen"] += 1
            continue

        print(f"    🎧 Primera escucha: {first_listen.isoformat()}")

        if not args.dry_run and task and task.get("ical_text"):
            ok = update_vtodo_completed(task, first_listen)
            if ok:
                print(f"    ✅ VTODO actualizado (COMPLETED={first_listen.isoformat()})")
                stats["listened_updated"] += 1
                # Actualizar music_stats.db
                purchase = task.get("purchase_date") or release_date
                update_music_db(music_conn, artist, album,
                                release_date, purchase, first_listen.isoformat())
                music_conn.commit()
        elif args.dry_run:
            print(f"    [DRY RUN] pondría COMPLETED={first_listen.isoformat()} en VTODO")
            stats["listened_updated"] += 1

    # ── 5. Resumen ────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("📊 Resumen:")
    print(f"  VTODOs creados:           {stats['vtodo_created']}")
    print(f"  Fechas de escucha nuevas: {stats['listened_updated']}")
    print(f"  Ya completados:           {stats['already_ok']}")
    print(f"  Sin escucha en Last.fm:   {stats['no_listen']}")

    if lastfm_conn:
        lastfm_conn.close()
    music_conn.close()
    print("\n✅ ¡Hecho!")


if __name__ == "__main__":
    main()
