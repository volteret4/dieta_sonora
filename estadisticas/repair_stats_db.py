#!/usr/bin/env python3
"""
repair_db.py — Rellena los campos vacíos de music_stats.db consultando
las fuentes disponibles en este orden de prioridad:

  release_date   → VEVENT en Radicale → MusicBrainz
  store_date     → copia de release_date
  purchase_date  → VTODO en Radicale  → Airsonic (getStarred2)
  listened_date  → VTODO COMPLETED en Radicale → Last.fm DB
                   (si se encuentra en Last.fm, actualiza también el VTODO)

Uso:
    python repair_db.py
    python repair_db.py --dry-run     # muestra qué haría sin escribir nada
    python repair_db.py --album-id 42 # repara solo ese álbum
    python repair_db.py --limit 20    # limita a N álbumes por ejecución

Variables .env necesarias:
    RADICALE_URL, RADICALE_USERNAME, RADICALE_PW, RADICALE_CALENDAR
    MB_EMAIL
    AIRSONIC_URL, AIRSONIC_USER, AIRSONIC_PW
    LASTFM_DB      (ruta a lastfm_stats.db)
    MUSIC_DB       (ruta a music_stats.db, defecto: music_stats.db)
"""

import argparse
import os
import re
import sqlite3
import sys
import time
import unicodedata
import uuid
from datetime import datetime, date, timezone
from typing import Optional
from xml.etree import ElementTree as ET

import requests
from sops_env import load_sops_env
from icalendar import Calendar, Todo, vDatetime, vText

load_sops_env()

# ─────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────
RADICALE_URL      = os.getenv("RADICALE_URL", "").rstrip("/")
RADICALE_USER     = os.getenv("RADICALE_USERNAME", "")
RADICALE_PW       = os.getenv("RADICALE_PW", "")
RADICALE_CALENDAR = os.getenv("RADICALE_CALENDAR", "/")

MB_EMAIL      = os.getenv("MB_EMAIL", "user@example.com")
MB_BASE       = "https://musicbrainz.org/ws/2/"
MB_UA         = f"RepairDB/1.0 ({MB_EMAIL})"
MB_RATE_LIMIT = 1.5

AIRSONIC_URL  = os.getenv("AIRSONIC_URL", "").rstrip("/")
AIRSONIC_USER = os.getenv("AIRSONIC_USER", "")
AIRSONIC_PW   = os.getenv("AIRSONIC_PASS", "")
AIRSONIC_CLIENT = "RepairDB"

LASTFM_DB = os.getenv("LASTFM_DB", "lastfm_stats.db")
MUSIC_DB  = os.getenv("MUSIC_DB",  "music_stats.db")


# ─────────────────────────────────────────────
#  HELPERS COMUNES
# ─────────────────────────────────────────────

def _normalize(s: str) -> str:
    """Lowercase + sin acentos + sin caracteres especiales + colapsar espacios."""
    s = re.sub(r"\s+", " ", s.strip().lower())
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    # eliminar caracteres no alfanuméricos para comparaciones fuzzy en Radicale
    return s


def _fuzzy(s: str) -> str:
    """Solo letras y dígitos, para comparar ignorando signos de puntuación."""
    n = _normalize(s)
    return re.sub(r"[^a-z0-9 ]", "", n).strip()


def _match(a: str, b: str) -> bool:
    """True si dos cadenas son equivalentes ignorando acentos y signos."""
    return _fuzzy(a) == _fuzzy(b)


def parse_ical_date(val) -> Optional[str]:
    """Convierte un valor icalendar a cadena ISO."""
    if val is None:
        return None
    if hasattr(val, "dt"):
        val = val.dt
    if isinstance(val, datetime):
        return val.date().isoformat()
    if isinstance(val, date):
        return val.isoformat()
    return None


def strip_emojis(s: str) -> str:
    return re.sub(
        r"^[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+"
        r"|[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+$",
        "", s,
    ).strip()


def parse_summary(summary: str) -> tuple[str, str]:
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
#  RADICALE — descarga general
# ─────────────────────────────────────────────

def _caldav_report(filter_comp: str) -> list[dict]:
    """
    Ejecuta un REPORT filtrando por tipo de componente (VEVENT o VTODO).
    Devuelve lista de {href, ical_text}.
    """
    url = RADICALE_URL + RADICALE_CALENDAR
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop><D:getetag/><C:calendar-data/></D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="{filter_comp}"/>
    </C:comp-filter>
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

    ns   = {"D": "DAV:", "C": "urn:ietf:params:xml:ns:caldav"}
    root = ET.fromstring(r.content)
    items = []
    for resp in root.findall(".//D:response", ns):
        href_el  = resp.find("D:href", ns)
        cal_data = resp.find(".//C:calendar-data", ns)
        etag_el  = resp.find(".//D:getetag", ns)
        if href_el is not None and cal_data is not None and cal_data.text:
            # Guardar ETag (sin comillas) para usarlo en If-Match al hacer PUT
            etag = (etag_el.text or "").strip().strip('"') if etag_el is not None else None
            items.append({
                "href":      href_el.text,
                "ical_text": cal_data.text,
                "etag":      etag,
            })
    return items


def _put_ical(href: str, ical_text: str, etag: Optional[str] = None) -> bool:
    url = href if href.startswith("http") else RADICALE_URL + href
    headers = {"Content-Type": "text/calendar; charset=utf-8"}
    # Radicale exige If-Match con el ETag actual para modificar items existentes.
    # Sin este header devuelve 403 aunque las credenciales sean correctas.
    if etag:
        headers["If-Match"] = f'"{etag}"'
    else:
        # If-Match: * acepta cualquier version (fallback si no tenemos el ETag)
        headers["If-Match"] = "*"
    r = requests.put(
        url,
        data=ical_text.encode("utf-8"),
        headers=headers,
        auth=(RADICALE_USER, RADICALE_PW),
        timeout=30,
    )
    if r.status_code not in (200, 201, 204):
        print(f"      ⚠ PUT {href} → HTTP {r.status_code}: {r.text[:120]}")
        return False
    return True


# ─────────────────────────────────────────────
#  RADICALE — VEVENTs (release dates)
# ─────────────────────────────────────────────

_vevent_cache: Optional[list[dict]] = None  # [{artist, album, release_date}]

def _load_vevents() -> list[dict]:
    global _vevent_cache
    if _vevent_cache is not None:
        return _vevent_cache

    print("  📅 Cargando VEVENTs de Radicale...")
    items = _caldav_report("VEVENT")
    result = []
    for item in items:
        try:
            cal = Calendar.from_ical(item["ical_text"])
        except Exception:
            continue
        for comp in cal.walk():
            if getattr(comp, "name", "") != "VEVENT":
                continue
            summary = str(comp.get("SUMMARY", ""))
            artist, album = parse_summary(summary)
            if not album:
                continue
            release_date = parse_ical_date(comp.get("DTSTART"))
            if release_date:
                result.append({"artist": artist, "album": album, "release_date": release_date})
    print(f"    → {len(result)} VEVENTs con fecha")
    _vevent_cache = result
    return result


def radicale_release_date(artist: str, album: str) -> Optional[str]:
    """Busca release_date en los VEVENTs de Radicale por artista+álbum (fuzzy)."""
    for ev in _load_vevents():
        if _match(ev["artist"], artist) and _match(ev["album"], album):
            return ev["release_date"]
    return None


# ─────────────────────────────────────────────
#  RADICALE — VTODOs (purchase / listened)
# ─────────────────────────────────────────────

_vtodo_cache: Optional[list[dict]] = None  # [{artist, album, purchase_date, listened_date, completed, href, ical_text}]

def _load_vtodos() -> list[dict]:
    global _vtodo_cache
    if _vtodo_cache is not None:
        return _vtodo_cache

    print("  📋 Cargando VTODOs de Radicale...")
    items = _caldav_report("VTODO")
    result = []
    for item in items:
        try:
            cal = Calendar.from_ical(item["ical_text"])
        except Exception:
            continue
        for comp in cal.walk():
            if getattr(comp, "name", "") != "VTODO":
                continue
            summary = str(comp.get("SUMMARY", ""))
            artist, album = parse_summary(summary)
            if not album:
                continue

            status        = str(comp.get("STATUS", "")).upper()
            purchase_date = (
                parse_ical_date(comp.get("CREATED"))
                or parse_ical_date(comp.get("DTSTART"))
            )
            listened_date = None
            is_completed  = status == "COMPLETED" or comp.get("COMPLETED") is not None
            if is_completed:
                listened_date = (
                    parse_ical_date(comp.get("COMPLETED"))
                    or parse_ical_date(comp.get("LAST-MODIFIED"))
                    or parse_ical_date(comp.get("DTSTAMP"))
                )

            result.append({
                "artist":        artist,
                "album":         album,
                "purchase_date": purchase_date,
                "listened_date": listened_date,
                "completed":     is_completed,
                "href":          item["href"],
                "etag":          item.get("etag"),
                "ical_text":     item["ical_text"],
            })
    print(f"    → {len(result)} VTODOs encontrados")
    _vtodo_cache = result
    return result


def radicale_vtodo(artist: str, album: str) -> Optional[dict]:
    """Devuelve el primer VTODO que coincida con artista+álbum (fuzzy)."""
    for t in _load_vtodos():
        if _match(t["artist"], artist) and _match(t["album"], album):
            return t
    return None


def radicale_mark_completed(vtodo: dict, listened_date: date) -> bool:
    """Actualiza el VTODO en Radicale añadiendo COMPLETED + STATUS:COMPLETED."""
    try:
        cal = Calendar.from_ical(vtodo["ical_text"])
    except Exception as e:
        print(f"      ⚠ Error parseando VTODO: {e}")
        return False

    updated_cal = Calendar()
    for k, v in cal.items():
        updated_cal.add(k, v)

    for comp in cal.walk():
        if comp.name == "VTODO":
            comp["STATUS"] = vText("COMPLETED")
            completed_dt = datetime.combine(listened_date, datetime.min.time(), tzinfo=timezone.utc)
            if "COMPLETED" not in comp:
                comp.add("COMPLETED", completed_dt)
            else:
                comp["COMPLETED"] = vDatetime(completed_dt)
            comp["LAST-MODIFIED"] = vDatetime(datetime.now(tz=timezone.utc))
            updated_cal.add_component(comp)
        elif comp.name != "VCALENDAR":
            updated_cal.add_component(comp)

    return _put_ical(vtodo["href"], updated_cal.to_ical().decode("utf-8"), vtodo.get("etag"))


# ─────────────────────────────────────────────
#  MUSICBRAINZ — release date
# ─────────────────────────────────────────────

_mb_last_call = 0.0
_mb_session   = requests.Session()
_mb_session.headers.update({"User-Agent": MB_UA})

try:
    import certifi
    _mb_session.verify = certifi.where()
except ImportError:
    pass

_LUCENE_SPECIAL = re.compile(r'([\+\-\!\(\)\{\}\[\]\^"~\*\?:\\\/])')

def _mb_escape(s: str) -> str:
    return _LUCENE_SPECIAL.sub(r"\\\1", s)


def _mb_get(endpoint: str, params: dict, _attempt: int = 0) -> Optional[dict]:
    global _mb_last_call, _mb_session
    MAX_RETRIES = 5

    elapsed = time.time() - _mb_last_call
    if elapsed < MB_RATE_LIMIT:
        time.sleep(MB_RATE_LIMIT - elapsed)

    try:
        r = _mb_session.get(MB_BASE + endpoint, params={**params, "fmt": "json"}, timeout=60)
    except (requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.SSLError,
            requests.exceptions.ChunkedEncodingError) as exc:
        _mb_last_call = time.time()
        if _attempt >= MAX_RETRIES:
            print(f"      ⚠ MB: error de red tras {MAX_RETRIES} reintentos ({exc.__class__.__name__})")
            return None
        wait = 5 * (2 ** _attempt)
        print(f"      MB: {exc.__class__.__name__}, reintento {_attempt+1}/{MAX_RETRIES} en {wait}s...")
        if isinstance(exc, requests.exceptions.SSLError):
            _mb_session.close()
            _mb_session = requests.Session()
            _mb_session.headers.update({"User-Agent": MB_UA})
        time.sleep(wait)
        return _mb_get(endpoint, params, _attempt + 1)

    _mb_last_call = time.time()

    if r.status_code in (400, 404):
        return None
    if r.status_code in (429, 503):
        wait = max(int(r.headers.get("Retry-After", 10 * (2 ** _attempt))), 10)
        if _attempt >= MAX_RETRIES:
            return None
        print(f"      MB: HTTP {r.status_code}, esperando {wait}s...")
        time.sleep(wait)
        return _mb_get(endpoint, params, _attempt + 1)
    if r.status_code in (500, 502, 504):
        if _attempt >= MAX_RETRIES:
            return None
        wait = 5 * (2 ** _attempt)
        time.sleep(wait)
        return _mb_get(endpoint, params, _attempt + 1)

    r.raise_for_status()
    return r.json()


def mb_release_date(artist: str, album: str) -> Optional[str]:
    """
    Busca en MusicBrainz la fecha de lanzamiento del álbum.
    Devuelve cadena ISO (YYYY-MM-DD o YYYY-MM o YYYY) o None.
    """
    aq = _mb_escape(artist)
    bq = _mb_escape(album)

    data = _mb_get("release-group", {
        "query": f'artist:"{aq}" AND release:"{bq}"',
        "limit": 5,
    })
    if not data:
        return None

    rgs = data.get("release-groups", [])
    if not rgs:
        return None

    # Preferir score 100, si no el primero
    best = rgs[0]
    for rg in rgs:
        if str(rg.get("score", 0)) == "100":
            best = rg
            break

    # first-release-date viene en el release-group
    frd = best.get("first-release-date", "")
    if frd:
        # Normalizar a YYYY-MM-DD si viene incompleto
        parts = frd.split("-")
        if len(parts) == 1:
            frd = f"{parts[0]}-01-01"
        elif len(parts) == 2:
            frd = f"{parts[0]}-{parts[1]}-01"
        return frd

    # Fallback: buscar en releases individuales
    rg_id = best.get("id")
    if not rg_id:
        return None
    detail = _mb_get(f"release-group/{rg_id}", {"inc": "releases"})
    if not detail:
        return None
    releases = detail.get("releases", [])
    dates = [r.get("date", "") for r in releases if r.get("date")]
    if not dates:
        return None
    dates.sort()
    frd = dates[0]
    parts = frd.split("-")
    if len(parts) == 1:
        frd = f"{parts[0]}-01-01"
    elif len(parts) == 2:
        frd = f"{parts[0]}-{parts[1]}-01"
    return frd


def mb_tracklist(artist: str, album: str) -> list[str]:
    """Devuelve lista de títulos de pistas normalizados."""
    aq = _mb_escape(artist)
    bq = _mb_escape(album)

    data = _mb_get("release", {
        "query": f'artist:"{aq}" AND release:"{bq}"',
        "limit": 3,
    })
    if not data or not data.get("releases"):
        return []

    releases = data["releases"]
    best = releases[0]
    for rel in releases:
        if str(rel.get("score", 0)) == "100":
            best = rel
            break

    mbid = best.get("id")
    if not mbid:
        return []

    detail = _mb_get(f"release/{mbid}", {"inc": "recordings"})
    if not detail:
        return []

    tracks = []
    for medium in detail.get("media", []):
        for track in medium.get("tracks", []):
            title = track.get("title") or (track.get("recording") or {}).get("title", "")
            if title:
                tracks.append(_normalize(title))
    return tracks


# ─────────────────────────────────────────────
#  AIRSONIC — purchase date (getStarred2)
# ─────────────────────────────────────────────

_airsonic_starred: Optional[list[dict]] = None  # [{artist, album, starred_date}]

def _load_airsonic_starred() -> list[dict]:
    global _airsonic_starred
    if _airsonic_starred is not None:
        return _airsonic_starred

    if not AIRSONIC_URL or not AIRSONIC_USER:
        _airsonic_starred = []
        return []

    print("  🎵 Cargando álbumes destacados de Airsonic...")
    try:
        r = requests.get(
            f"{AIRSONIC_URL}/rest/getStarred2.view",
            params={
                "u": AIRSONIC_USER,
                "p": AIRSONIC_PW,
                "v": "1.15.0",
                "c": AIRSONIC_CLIENT,
                "f": "json",
            },
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
        subsonic_response = data.get("subsonic-response", {})

        if subsonic_response.get("status") != "ok":
            error = subsonic_response.get("error", {})
            print(f"    ⚠ Airsonic error: {error.get('message', 'desconocido')}")
            _airsonic_starred = []
            return []

        starred = subsonic_response.get("starred2", {})
        albums  = starred.get("album", [])
        if isinstance(albums, dict):   # un solo álbum devuelto como dict
            albums = [albums]

        result = []
        for alb in albums:
            artist = alb.get("artist", "")
            name   = alb.get("name", alb.get("title", ""))
            starred_date = alb.get("starred", "")
            # starred viene como ISO 8601 con hora: "2024-03-15T10:30:00.000Z"
            if starred_date:
                try:
                    starred_date = datetime.fromisoformat(
                        starred_date.replace("Z", "+00:00")
                    ).date().isoformat()
                except ValueError:
                    starred_date = starred_date[:10]  # tomar solo YYYY-MM-DD
            result.append({
                "artist":      artist,
                "album":       name,
                "starred_date": starred_date or None,
            })

        print(f"    → {len(result)} álbumes con estrella en Airsonic")
        _airsonic_starred = result
        return result

    except Exception as e:
        print(f"    ⚠ Error Airsonic: {e}")
        _airsonic_starred = []
        return []


def airsonic_purchase_date(artist: str, album: str) -> Optional[str]:
    """Devuelve la fecha en que el álbum fue marcado como estrella en Airsonic."""
    for entry in _load_airsonic_starred():
        if _match(entry["artist"], artist) and _match(entry["album"], album):
            return entry["starred_date"]
    return None


# ─────────────────────────────────────────────
#  LAST.FM DB — primera escucha
# ─────────────────────────────────────────────

def lastfm_first_listen(
    conn: sqlite3.Connection, artist: str, tracks: list[str]
) -> Optional[date]:
    """Busca en lastfm_stats.db la fecha del primer scrobble de alguna pista."""
    if not tracks:
        return None

    artist_key = _normalize(artist)
    row = conn.execute(
        "SELECT artist_id FROM artists WHERE name_normalized = ?", (artist_key,)
    ).fetchone()
    if not row:
        row = conn.execute(
            "SELECT artist_id FROM artists WHERE name_normalized LIKE ?",
            (f"%{artist_key}%",),
        ).fetchone()
    if not row:
        return None

    artist_id    = row[0]
    placeholders = ",".join("?" * len(tracks))

    result = conn.execute(
        f"""SELECT MIN(ts), MIN(ts_iso)
            FROM scrobbles
            WHERE artist_id = ?
              AND track_normalized IN ({placeholders})""",
        [artist_id, *tracks],
    ).fetchone()

    if result and result[0]:
        try:
            return datetime.fromisoformat(result[1]).date()
        except Exception:
            return datetime.fromtimestamp(result[0], tz=timezone.utc).date()

    # Búsqueda difusa por primera palabra larga de cada pista
    earliest = None
    for track in tracks[:10]:
        words = [w for w in track.split() if len(w) > 3]
        if not words:
            continue
        res = conn.execute(
            """SELECT MIN(ts), MIN(ts_iso) FROM scrobbles
               WHERE artist_id = ? AND track_normalized LIKE ?""",
            (artist_id, f"%{words[0]}%"),
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
#  BASE DE DATOS — lectura y escritura
# ─────────────────────────────────────────────

def load_incomplete_albums(conn: sqlite3.Connection, album_id: Optional[int] = None) -> list[dict]:
    """
    Devuelve los álbumes que tienen al menos un campo de fecha NULL.
    Si se pasa album_id, filtra solo ese álbum.
    """
    where = "WHERE (al.release_date IS NULL OR al.store_date IS NULL " \
            "       OR al.purchase_date IS NULL OR al.listened_date IS NULL)"
    params = []
    if album_id is not None:
        where += " AND al.album_id = ?"
        params.append(album_id)

    cur = conn.execute(f"""
        SELECT al.album_id,
               ar.name  AS artist,
               al.name  AS album,
               al.release_date,
               al.store_date,
               al.purchase_date,
               al.listened_date
        FROM   albums  al
        JOIN   artists ar ON ar.artist_id = al.artist_id
        {where}
        ORDER  BY al.album_id
    """, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def save_dates(
    conn: sqlite3.Connection,
    album_id: int,
    release_date:  Optional[str],
    store_date:    Optional[str],
    purchase_date: Optional[str],
    listened_date: Optional[str],
    dry_run: bool,
) -> bool:
    """Actualiza las fechas del álbum y recalcula los días intermedios."""
    if dry_run:
        return True
    conn.execute("""
        UPDATE albums SET
            release_date              = ?,
            store_date                = ?,
            purchase_date             = ?,
            listened_date             = ?,
            days_release_to_store     = ?,
            days_store_to_purchase    = ?,
            days_purchase_to_listened = ?
        WHERE album_id = ?
    """, (
        release_date, store_date, purchase_date, listened_date,
        days_between(release_date, store_date),
        days_between(store_date,   purchase_date),
        days_between(purchase_date, listened_date),
        album_id,
    ))
    return True


# ─────────────────────────────────────────────
#  LÓGICA PRINCIPAL POR ÁLBUM
# ─────────────────────────────────────────────

def repair_album(
    row: dict,
    lastfm_conn: Optional[sqlite3.Connection],
    music_conn: sqlite3.Connection,
    dry_run: bool,
) -> dict:
    """
    Rellena los campos vacíos de un álbum y devuelve un resumen de cambios.
    """
    artist  = row["artist"]
    album   = row["album"]
    alb_id  = row["album_id"]

    release_date  = row["release_date"]
    store_date    = row["store_date"]
    purchase_date = row["purchase_date"]
    listened_date = row["listened_date"]

    changes = []

    # ── 1. RELEASE DATE ───────────────────────────────────────────────────────
    if not release_date:
        # a) Radicale VEVENT
        release_date = radicale_release_date(artist, album)
        if release_date:
            changes.append(f"release_date={release_date} (Radicale)")
        else:
            # b) MusicBrainz
            release_date = mb_release_date(artist, album)
            if release_date:
                changes.append(f"release_date={release_date} (MusicBrainz)")
            else:
                print(f"    ⚠ Sin release_date para {artist} — {album}")

    # ── 2. STORE DATE ────────────────────────────────────────────────────────
    if not store_date and release_date:
        store_date = release_date
        changes.append(f"store_date={store_date} (copia release_date)")

    # ── 3. PURCHASE DATE ─────────────────────────────────────────────────────
    if not purchase_date:
        # a) Radicale VTODO
        vtodo = radicale_vtodo(artist, album)
        if vtodo and vtodo.get("purchase_date"):
            purchase_date = vtodo["purchase_date"]
            changes.append(f"purchase_date={purchase_date} (Radicale VTODO)")
        else:
            # b) Airsonic getStarred2
            purchase_date = airsonic_purchase_date(artist, album)
            if purchase_date:
                changes.append(f"purchase_date={purchase_date} (Airsonic)")
            else:
                print(f"    ⚠ Sin purchase_date para {artist} — {album}")

    # ── 4. LISTENED DATE ─────────────────────────────────────────────────────
    if not listened_date:
        vtodo = radicale_vtodo(artist, album)

        # a) VTODO ya completado en Radicale
        if vtodo and vtodo.get("completed") and vtodo.get("listened_date"):
            listened_date = vtodo["listened_date"]
            changes.append(f"listened_date={listened_date} (Radicale VTODO COMPLETED)")

        # b) Buscar en Last.fm DB
        elif lastfm_conn is not None:
            print(f"    🔍 Buscando primera escucha en Last.fm para {artist} — {album}...")
            tracks = mb_tracklist(artist, album)
            if tracks:
                first_listen = lastfm_first_listen(lastfm_conn, artist, tracks)
                if first_listen:
                    listened_date = first_listen.isoformat()
                    changes.append(f"listened_date={listened_date} (Last.fm DB)")

                    # Marcar VTODO como completado en Radicale
                    if vtodo and not dry_run:
                        ok = radicale_mark_completed(vtodo, first_listen)
                        if ok:
                            changes.append("VTODO marcado COMPLETED en Radicale")
                            # Invalidar caché para que la próxima llamada vea el cambio
                            global _vtodo_cache
                            _vtodo_cache = None
                        else:
                            print(f"    ⚠ No se pudo actualizar el VTODO en Radicale")
                    elif vtodo and dry_run:
                        changes.append("[dry-run] VTODO se marcaría COMPLETED en Radicale")
                else:
                    print(f"    ℹ Aún no escuchado en Last.fm")
            else:
                print(f"    ⚠ No se encontró tracklist en MusicBrainz para buscar en Last.fm")
        else:
            print(f"    ℹ Last.fm DB no disponible, no se puede buscar fecha de escucha")

    # ── Guardar si hay cambios ────────────────────────────────────────────────
    if changes:
        save_dates(music_conn, alb_id,
                   release_date, store_date, purchase_date, listened_date,
                   dry_run)
        if not dry_run:
            music_conn.commit()

    return {"changes": changes, "artist": artist, "album": album}


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Rellena campos vacíos en music_stats.db desde Radicale, "
                    "MusicBrainz, Airsonic y Last.fm."
    )
    parser.add_argument("--dry-run",  action="store_true",
                        help="Muestra qué haría sin modificar nada")
    parser.add_argument("--album-id", type=int, default=None,
                        help="Repara solo el álbum con ese album_id")
    parser.add_argument("--limit",    type=int, default=None,
                        help="Procesa como máximo N álbumes")
    args = parser.parse_args()

    print("🔧 repair_db.py — Reparación de fechas faltantes")
    if args.dry_run:
        print("   [DRY RUN — no se escribirá nada]")
    print("=" * 60)

    # Validaciones mínimas
    if not RADICALE_URL or not RADICALE_USER:
        print("❌ Configura RADICALE_URL y RADICALE_USERNAME en .env")
        sys.exit(1)

    # Conectar a music_stats.db
    if not os.path.exists(MUSIC_DB):
        print(f"❌ No se encuentra la base de datos: {MUSIC_DB}")
        sys.exit(1)
    music_conn = sqlite3.connect(MUSIC_DB)
    music_conn.execute("PRAGMA foreign_keys=ON")
    music_conn.execute("PRAGMA journal_mode=WAL")

    # Conectar a lastfm_stats.db (opcional)
    lastfm_conn = None
    if os.path.exists(LASTFM_DB):
        lastfm_conn = sqlite3.connect(LASTFM_DB)
        lastfm_conn.execute("PRAGMA journal_mode=WAL")
        print(f"💾 Last.fm DB: {LASTFM_DB}")
    else:
        print(f"⚠ Last.fm DB no encontrada ({LASTFM_DB}) — se omitirá fecha de escucha")

    # Cargar álbumes incompletos
    rows = load_incomplete_albums(music_conn, args.album_id)
    if args.limit:
        rows = rows[: args.limit]

    print(f"\n🎵 {len(rows)} álbumes con campos vacíos a revisar\n")

    if not rows:
        print("✅ Nada que reparar.")
        music_conn.close()
        if lastfm_conn:
            lastfm_conn.close()
        return

    # Pre-cargar Radicale y Airsonic (una sola petición para todos)
    _load_vevents()
    _load_vtodos()
    _load_airsonic_starred()
    print()

    # Procesar álbum a álbum
    stats = {"repaired": 0, "partial": 0, "unchanged": 0}

    for row in rows:
        label = f"[{row['album_id']}] {row['artist']} — {row['album']}"
        missing = [f for f in ("release_date", "store_date", "purchase_date", "listened_date")
                   if not row[f]]
        print(f"── {label}")
        print(f"   Faltan: {', '.join(missing)}")

        result = repair_album(row, lastfm_conn, music_conn, args.dry_run)

        if result["changes"]:
            for c in result["changes"]:
                print(f"   ✅ {c}")
            # ¿Todavía quedan campos vacíos?
            still_missing = len(missing) - sum(
                1 for c in result["changes"]
                if any(f in c for f in ("release_date", "store_date", "purchase_date", "listened_date"))
            )
            if still_missing > 0:
                stats["partial"] += 1
            else:
                stats["repaired"] += 1
        else:
            print("   ℹ Sin datos nuevos encontrados")
            stats["unchanged"] += 1
        print()

    # Resumen
    print("=" * 60)
    print("📊 Resumen:")
    print(f"  Reparados completamente: {stats['repaired']}")
    print(f"  Reparados parcialmente:  {stats['partial']}")
    print(f"  Sin cambios:             {stats['unchanged']}")
    if args.dry_run:
        print("\n  (dry-run: no se ha modificado ningún fichero ni base de datos)")

    music_conn.close()
    if lastfm_conn:
        lastfm_conn.close()
    print("\n✅ ¡Hecho!")


if __name__ == "__main__":
    main()
