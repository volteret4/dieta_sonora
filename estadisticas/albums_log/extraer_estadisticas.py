#!/usr/bin/env python3
"""
Music Calendar Extractor
Extracts data from Radicale (CalDAV) + store CSV + MusicBrainz
and stores everything in SQLite, then exports data.json for the dashboard.

Usage:
    pip install caldav icalendar requests
    python extract.py
"""
import sqlite3
import csv
import json
import time
import re
import os
from datetime import datetime, date
from typing import Optional
from dotenv import load_dotenv
import requests

load_dotenv()

# ─────────────────────────────────────────────
#  CONFIGURATION — edit these values
# ─────────────────────────────────────────────
RADICALE_URL      = os.getenv("RADICALE_URL")
RADICALE_USER     = os.getenv("RADICALE_USERNAME")
RADICALE_PASSWORD = os.getenv("RADICALE_PW")
CALENDAR_PATH     = ""    # path within Radicale

DB_PATH           = "music_stats.db"
JSON_PATH         = "stats.json"

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
    purchase_date             TEXT,
    listened_date             TEXT,
    days_release_to_purchase  INTEGER,
    days_purchase_to_listened INTEGER,
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
#  CALDAV FETCHING
# ─────────────────────────────────────────────

def fetch_caldav_raw() -> str:
    """Download the full calendar .ics via GET (Radicale supports this)."""
    url = RADICALE_URL.rstrip("/") + CALENDAR_PATH
    r = requests.get(url, auth=(RADICALE_USER, RADICALE_PASSWORD), timeout=30)
    r.raise_for_status()
    return r.text

def fetch_caldav_items() -> list[dict]:
    """
    Use REPORT to get all VEVENT and VTODO from the calendar.
    Returns list of raw icalendar component texts.
    """
    url = RADICALE_URL.rstrip("/") + CALENDAR_PATH
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

    headers = {
        "Depth": "1",
        "Content-Type": "application/xml; charset=utf-8",
    }
    r = requests.request(
        "REPORT", url,
        data=body.encode("utf-8"),
        headers=headers,
        auth=(RADICALE_USER, RADICALE_PASSWORD),
        timeout=30,
    )
    r.raise_for_status()

    # Extract calendar-data blocks from the XML response
    from xml.etree import ElementTree as ET
    root = ET.fromstring(r.content)
    ns = {
        "D": "DAV:",
        "C": "urn:ietf:params:xml:ns:caldav",
    }
    items = []
    for resp in root.findall(".//D:response", ns):
        cal_data = resp.find(".//C:calendar-data", ns)
        if cal_data is not None and cal_data.text:
            items.append(cal_data.text)
    return items

# ─────────────────────────────────────────────
#  ICALENDAR PARSING
# ─────────────────────────────────────────────

def parse_date(dt_value) -> Optional[str]:
    """Convert icalendar date/datetime to ISO string."""
    if dt_value is None:
        return None
    if hasattr(dt_value, 'dt'):
        dt_value = dt_value.dt
    if isinstance(dt_value, datetime):
        return dt_value.date().isoformat()
    if isinstance(dt_value, date):
        return dt_value.isoformat()
    return str(dt_value)

def strip_non_text(s: str) -> str:
    """
    Remove leading/trailing emojis, symbols and extra whitespace.
    Covers emoji blocks, dingbats, misc symbols (💿 📀 🎵 etc.)
    """
    return re.sub(
        r'^[\U00010000-\U0010ffff\u2000-\u2BFF\u2600-\u26FF\u2700-\u27BF\s]+'
        r'|[\U00010000-\U0010ffff\u2000-\u2BFF\u2600-\u26FF\u2700-\u27BF\s]+$',
        '', s
    ).strip()


def parse_summary(summary: str) -> tuple[str, str]:
    """Split 'Artist - Album' → (artist, album). Strips leading/trailing emojis."""
    summary = strip_non_text(summary)
    parts = re.split(r'\s+[-–—]\s+', summary, maxsplit=1)
    if len(parts) == 2:
        return strip_non_text(parts[0]), strip_non_text(parts[1])
    return summary, ""

def parse_calendar_items(raw_items: list[str]) -> tuple[dict, dict]:
    """
    Returns:
        events: {(artist, album): release_date}
        tasks:  {(artist, album): {"purchase_date": ..., "listened_date": ...}}
    """
    try:
        from icalendar import Calendar
    except ImportError:
        raise ImportError("Run: pip install icalendar")

    events = {}
    tasks  = {}

    for raw in raw_items:
        try:
            cal = Calendar.from_ical(raw)
        except Exception as e:
            print(f"  ⚠ Could not parse calendar item: {e}")
            continue

        for component in cal.walk():
            ctype = component.name

            if ctype == "VEVENT":
                summary = str(component.get("SUMMARY", ""))
                artist, album = parse_summary(summary)
                if not album:
                    continue
                release_date = parse_date(component.get("DTSTART"))
                events[(artist.lower(), album.lower())] = {
                    "artist": artist,
                    "album": album,
                    "release_date": release_date,
                }

            elif ctype == "VTODO":
                summary = str(component.get("SUMMARY", ""))
                artist, album = parse_summary(summary)
                if not album:
                    continue

                status = str(component.get("STATUS", "")).upper()
                # Task created = purchase date (CREATED or DTSTART)
                purchase_date = parse_date(component.get("CREATED")) or \
                                parse_date(component.get("DTSTART"))
                # Task completed = listened date
                # Some CalDAV clients set STATUS=COMPLETED but omit the COMPLETED
                # property, so we fall back to LAST-MODIFIED or DTSTAMP.
                listened_date = None
                if status == "COMPLETED":
                    listened_date = (
                        parse_date(component.get("COMPLETED"))
                        or parse_date(component.get("LAST-MODIFIED"))
                        or parse_date(component.get("DTSTAMP"))
                    )

                tasks[(artist.lower(), album.lower())] = {
                    "artist": artist,
                    "album": album,
                    "purchase_date": purchase_date,
                    "listened_date": listened_date,
                }

    return events, tasks

# ─────────────────────────────────────────────
#  CSV PARSING
# ─────────────────────────────────────────────


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
#  DATA MERGING
# ─────────────────────────────────────────────

def days_between(d1: Optional[str], d2: Optional[str]) -> Optional[int]:
    if not d1 or not d2:
        return None
    try:
        a = date.fromisoformat(d1)
        b = date.fromisoformat(d2)
        return (b - a).days
    except Exception:
        return None

def merge_data(events: dict, tasks: dict) -> list[dict]:
    """Combine all sources into a list of album records."""
    all_keys = set(events.keys()) | set(tasks.keys())

    records = []
    for key in all_keys:
        ev   = events.get(key, {})
        task = tasks.get(key, {})

        artist = ev.get("artist") or task.get("artist") or key[0]
        album  = ev.get("album")  or task.get("album")  or key[1]

        release_date  = ev.get("release_date")
        purchase_date = task.get("purchase_date")
        listened_date = task.get("listened_date")

        records.append({
            "artist":        artist,
            "album":         album,
            "genre":         None,  # filled in later
            "release_date":  release_date,
            "purchase_date": purchase_date,
            "listened_date": listened_date,
            "days_release_to_purchase":  days_between(release_date, purchase_date),
            "days_purchase_to_listened": days_between(purchase_date, listened_date),
        })

    return records

# ─────────────────────────────────────────────
#  SQLITE STORAGE
# ─────────────────────────────────────────────

# ─────────────────────────────────────────────
#  SQLITE STORAGE
# ─────────────────────────────────────────────

def save_record(conn: sqlite3.Connection, rec: dict) -> str:
    """
    Persist one album using the normalized schema.

    Steps:
      1. get_or_create artist            → artist_id
      2. get_or_create genre (if known)  → genre_id
      3. link artist ↔ genre             (idempotent)
      4. Check album existence by (artist_id, name_normalized):
           NOT EXISTS → full insert, return 'created'
           EXISTS     → update only mutable date fields if any changed,
                        return 'updated' or 'skipped'
    """
    artist_id = get_or_create_artist(conn, rec["artist"])

    genre_id = None
    if rec.get("genre"):
        genre_id = get_or_create_genre(conn, rec["genre"])
        link_artist_genre(conn, artist_id, genre_id)

    name_norm = _normalize(rec["album"])

    existing = conn.execute(
        """SELECT album_id, release_date, purchase_date, listened_date
           FROM albums
           WHERE artist_id = ? AND name_normalized = ?""",
        (artist_id, name_norm),
    ).fetchone()

    if existing is None:
        # ── New album ─────────────────────────────────────────────────────
        conn.execute("""
            INSERT INTO albums
                (artist_id, genre_id, name, name_normalized,
                 release_date, purchase_date, listened_date,
                 days_release_to_purchase, days_purchase_to_listened)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            artist_id, genre_id, rec["album"], name_norm,
            rec.get("release_date"),
            rec.get("purchase_date"), rec.get("listened_date"),
            rec.get("days_release_to_purchase"),
            rec.get("days_purchase_to_listened"),
        ))
        return "created"

    # ── Album already exists: update any field that has improved data ────
    album_id, old_release, old_purchase, old_listened = existing

    # Never overwrite a date/value already stored with None
    new_release  = rec.get("release_date")  or old_release
    new_purchase = rec.get("purchase_date") or old_purchase
    new_listened = rec.get("listened_date") or old_listened

    # Also update genre_id if we now have one and didn't before
    existing_genre = conn.execute(
        "SELECT genre_id FROM albums WHERE album_id = ?", (album_id,)
    ).fetchone()
    old_genre_id = existing_genre[0] if existing_genre else None
    new_genre_id = genre_id if genre_id is not None else old_genre_id

    changed = (
        (new_purchase, new_listened, new_release, new_genre_id)
        != (old_purchase, old_listened, old_release, old_genre_id)
    )
    if not changed:
        return "skipped"

    conn.execute("""
        UPDATE albums SET
            release_date              = ?,
            purchase_date             = ?,
            listened_date             = ?,
            genre_id                  = ?,
            days_release_to_purchase  = ?,
            days_purchase_to_listened = ?
        WHERE album_id = ?
    """, (
        new_release, new_purchase, new_listened,
        new_genre_id,
        days_between(new_release, new_purchase),
        days_between(new_purchase, new_listened),
        album_id,
    ))
    return "updated"


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
            al.purchase_date,
            al.listened_date,
            al.days_release_to_purchase,
            al.days_purchase_to_listened
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
    print("🎵 Music Calendar Extractor")
    print("=" * 40)

    # 1. Fetch CalDAV
    print("\n📅 Fetching calendar from Radicale...")
    try:
        raw_items = fetch_caldav_items()
        print(f"  Found {len(raw_items)} calendar items")
    except Exception as e:
        print(f"  ❌ CalDAV error: {e}")
        print("  Make sure RADICALE_URL, RADICALE_USER, RADICALE_PASSWORD and CALENDAR_PATH are correct.")
        return

    # 2. Parse
    print("\n🔍 Parsing events and tasks...")
    events, tasks = parse_calendar_items(raw_items)
    print(f"  VEVENT (releases):  {len(events)}")
    print(f"  VTODO  (purchases): {len(tasks)}")

    # 3. Merge all sources
    print("\n🔗 Merging data...")
    records = merge_data(events, tasks)
    print(f"  Total unique albums: {len(records)}")

    # 5. Open DB (creates tables on first run, idempotent afterwards)
    print(f"\n💾 Opening {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON")
    init_db(conn)

    # 6. Genre lookup — skip albums already stored to avoid redundant API calls
    print("\n🌐 Fetching genres (new albums only)...")
    for rec in records:
        artist_id  = get_or_create_artist(conn, rec["artist"])
        name_norm  = _normalize(rec["album"])
        cached     = conn.execute(
            """SELECT g.name FROM albums al
               LEFT JOIN genres g ON g.genre_id = al.genre_id
               WHERE al.artist_id = ? AND al.name_normalized = ?""",
            (artist_id, name_norm),
        ).fetchone()

        if cached is not None and cached[0] is not None:
            rec["genre"] = cached[0]          # already in DB with a genre, reuse it
        else:
            # Either new album or existing album with no genre yet → fetch
            rec["genre"] = get_genre_from_musicbrainz(rec["artist"], rec["album"])

    # 7. Persist — normalized insert/update with dedup
    print("\n📥 Saving to database...")
    stats = {"created": 0, "updated": 0, "skipped": 0}
    for rec in records:
        result = save_record(conn, rec)
        stats[result] += 1
        if result == "created":
            print(f"  + {rec['artist']} — {rec['album']}")
        elif result == "updated":
            print(f"  ↺ {rec['artist']} — {rec['album']}  (dates updated)")
    conn.commit()
    print(f"  Created: {stats['created']}  |  Updated: {stats['updated']}  |  Unchanged: {stats['skipped']}")

    # 8. JSON export for dashboard
    print(f"\n📤 Exporting {JSON_PATH}...")
    export_json(conn, JSON_PATH)
    conn.close()

    print("\n✅ Done! Open estadisticas.html in your browser.")

if __name__ == "__main__":
    main()
