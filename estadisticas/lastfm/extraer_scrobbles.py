#!/usr/bin/env python3
"""
Scrobble Extractor — Last.fm + ListenBrainz
Descarga historial de ambos servicios, deduplica y exporta:
  - lastfm_stats.json   (carga inicial: resúmenes + matriz horaria)
  - lastfm_detail.json  (carga perezosa: detalle por entidad)

Uso:
    pip install requests python-dotenv
    python extraer_scrobbles.py

Variables de entorno (.env):
    LASTFM_API_KEY       — API key de Last.fm (requerida para Last.fm)
    LASTFM_USERNAME      — usuario de Last.fm
    LB_USERNAME          — usuario de ListenBrainz
    LB_TOKEN             — token de ListenBrainz (opcional, para cuentas privadas)

Deduplicación:
  - Mismo servicio: UNIQUE(artist_id, track_normalized, ts) exacto.
  - Entre servicios: ventana de ±DEDUP_WINDOW_SECS segundos con mismo
    artista normalizado y pista normalizada. El registro del servicio
    primario (Last.fm) tiene precedencia; el de LB se descarta si ya
    existe uno dentro de la ventana.

Resiliencia:
  - Volcado incremental cada SAVE_EVERY páginas/lotes directamente en la BD.
  - Tabla sync_state guarda el cursor de cada servicio para poder reanudar
    exactamente donde se interrumpió, sin perder ningún scrobble ya descargado.
  - Reintentos automáticos con backoff exponencial ante fallos de red.
"""

import sqlite3, json, time, re, os
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests

load_dotenv()

# ─────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────
LASTFM_API_KEY  = os.getenv("LASTFM_API_KEY", "")
LASTFM_USERNAME = os.getenv("LASTFM_USERNAME", "")
LB_USERNAME     = os.getenv("LB_USERNAME", "")
LB_TOKEN        = os.getenv("LB_TOKEN", "")   # opcional

LASTFM_BASE  = "https://ws.audioscrobbler.com/2.0/"
LB_BASE      = "https://api.listenbrainz.org/1/"
MB_BASE      = "https://musicbrainz.org/ws/2/"
MB_UA        = "ScrobbleExtractor/1.0 (your@email.com)"
MB_RATE_LIMIT = 1.1

DB_PATH          = "lastfm_stats.db"
JSON_PATH        = "lastfm_stats.json"
DETAIL_JSON_PATH = "lastfm_detail.json"

# Ventana de dedup entre servicios (segundos).
DEDUP_WINDOW_SECS = 120

# Guardar en BD cada cuántas páginas (Last.fm) o lotes (LB).
# Aumenta este valor si la BD es muy lenta, redúcelo para mayor seguridad.
SAVE_EVERY = 5   # ~1000 scrobbles para LFM (200/página), ~500 para LB (100/lote)

MATRIX_TOP_ARTISTS = 25
MATRIX_TOP_ALBUMS  = 25
MATRIX_TOP_GENRES  = 20

GENRE_BLACKLIST = {
    "seen live", "albums i own", "favorite", "favourites", "beautiful",
    "music", "amazing", "best", "awesome", "love",
}

# ─────────────────────────────────────────────
#  SCHEMA
# ─────────────────────────────────────────────
SCHEMA_TABLES = """
CREATE TABLE IF NOT EXISTS artists (
    artist_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL,
    name_normalized  TEXT NOT NULL UNIQUE,
    mbid             TEXT,
    genres_fetched   INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS albums (
    album_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id        INTEGER NOT NULL REFERENCES artists(artist_id),
    name             TEXT NOT NULL,
    name_normalized  TEXT NOT NULL,
    mbid             TEXT,
    UNIQUE(artist_id, name_normalized)
);
CREATE TABLE IF NOT EXISTS genres (
    genre_id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name             TEXT NOT NULL,
    name_normalized  TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS artist_genres (
    artist_id  INTEGER NOT NULL REFERENCES artists(artist_id),
    genre_id   INTEGER NOT NULL REFERENCES genres(genre_id),
    PRIMARY KEY (artist_id, genre_id)
);
CREATE TABLE IF NOT EXISTS scrobbles (
    scrobble_id      INTEGER PRIMARY KEY AUTOINCREMENT,
    artist_id        INTEGER NOT NULL REFERENCES artists(artist_id),
    album_id         INTEGER REFERENCES albums(album_id),
    track            TEXT NOT NULL,
    track_normalized TEXT NOT NULL DEFAULT '',
    ts               INTEGER NOT NULL,
    ts_iso           TEXT NOT NULL,
    source           TEXT NOT NULL DEFAULT 'lastfm'
);
CREATE TABLE IF NOT EXISTS sync_state (
    key    TEXT PRIMARY KEY,
    value  TEXT
);
"""

SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_scrobbles_ts       ON scrobbles(ts);
CREATE INDEX IF NOT EXISTS idx_scrobbles_artist   ON scrobbles(artist_id);
CREATE INDEX IF NOT EXISTS idx_scrobbles_album    ON scrobbles(album_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_scrobbles_dedup
    ON scrobbles(artist_id, track_normalized, ts);
"""

def _normalize(s: str) -> str:
    return re.sub(r'\s+', ' ', s.strip().lower())

def init_db(conn: sqlite3.Connection):
    conn.executescript(SCHEMA_TABLES)

    # Migraciones sobre BDs antiguas
    cols = {r[1] for r in conn.execute("PRAGMA table_info(scrobbles)")}
    if "source" not in cols:
        conn.execute("ALTER TABLE scrobbles ADD COLUMN source TEXT NOT NULL DEFAULT 'lastfm'")
        print("  ↑ Migración: columna 'source' añadida a scrobbles")
    if "track_normalized" not in cols:
        conn.execute("ALTER TABLE scrobbles ADD COLUMN track_normalized TEXT NOT NULL DEFAULT ''")
        conn.execute("UPDATE scrobbles SET track_normalized = lower(trim(track)) WHERE track_normalized = ''")
        print("  ↑ Migración: columna 'track_normalized' añadida y populada")
    conn.commit()

    conn.executescript(SCHEMA_INDEXES)
    conn.commit()

# ─────────────────────────────────────────────
#  SYNC STATE (cursor de reanudación)
# ─────────────────────────────────────────────
def state_get(conn, key: str):
    row = conn.execute("SELECT value FROM sync_state WHERE key=?", (key,)).fetchone()
    return row[0] if row else None

def state_set(conn, key: str, value):
    conn.execute("INSERT OR REPLACE INTO sync_state (key, value) VALUES (?,?)", (key, str(value)))
    conn.commit()

def state_del(conn, *keys):
    for k in keys:
        conn.execute("DELETE FROM sync_state WHERE key=?", (k,))
    conn.commit()

# ─────────────────────────────────────────────
#  BD HELPERS
# ─────────────────────────────────────────────
def get_or_create_artist(conn, name, mbid=None):
    key = _normalize(name)
    row = conn.execute("SELECT artist_id FROM artists WHERE name_normalized=?", (key,)).fetchone()
    if row: return row[0]
    return conn.execute(
        "INSERT INTO artists (name,name_normalized,mbid) VALUES (?,?,?)",
        (name, key, mbid)
    ).lastrowid

def get_or_create_album(conn, artist_id, name, mbid=None):
    key = _normalize(name)
    row = conn.execute(
        "SELECT album_id FROM albums WHERE artist_id=? AND name_normalized=?",
        (artist_id, key)
    ).fetchone()
    if row: return row[0]
    return conn.execute(
        "INSERT INTO albums (artist_id,name,name_normalized,mbid) VALUES (?,?,?,?)",
        (artist_id, name, key, mbid)
    ).lastrowid

def get_or_create_genre(conn, name):
    key = _normalize(name)
    row = conn.execute("SELECT genre_id FROM genres WHERE name_normalized=?", (key,)).fetchone()
    if row: return row[0]
    return conn.execute(
        "INSERT INTO genres (name,name_normalized) VALUES (?,?)", (name, key)
    ).lastrowid

def link_artist_genre(conn, artist_id, genre_id):
    conn.execute("INSERT OR IGNORE INTO artist_genres VALUES (?,?)", (artist_id, genre_id))

# ─────────────────────────────────────────────
#  DEDUP Y GUARDADO
# ─────────────────────────────────────────────
def is_duplicate(conn, artist_id: int, track_norm: str, ts: int) -> bool:
    """True si ya existe un scrobble del mismo artista+pista en ventana ±DEDUP_WINDOW_SECS."""
    row = conn.execute(
        """SELECT 1 FROM scrobbles
           WHERE artist_id = ?
             AND track_normalized = ?
             AND ts BETWEEN ? AND ?
           LIMIT 1""",
        (artist_id, track_norm, ts - DEDUP_WINDOW_SECS, ts + DEDUP_WINDOW_SECS)
    ).fetchone()
    return row is not None

def save_scrobbles(conn, scrobbles: list[dict], source: str = "lastfm") -> dict:
    """
    Inserta una lista de scrobbles evitando duplicados (exactos y de ventana cruzada).
    NO hace commit — el llamador decide cuándo commitear.
    """
    stats = {"new": 0, "dup_exact": 0, "dup_cross": 0, "no_artist": 0}

    for s in scrobbles:
        if not s.get("artist"):
            stats["no_artist"] += 1
            continue

        artist_id  = get_or_create_artist(conn, s["artist"])
        album_id   = get_or_create_album(conn, artist_id, s["album"]) if s.get("album") else None
        track_norm = _normalize(s["track"]) if s.get("track") else ""

        # Comprobación de ventana para fuentes secundarias (LB)
        if source != "lastfm" and is_duplicate(conn, artist_id, track_norm, s["ts"]):
            stats["dup_cross"] += 1
            continue

        try:
            conn.execute(
                """INSERT INTO scrobbles
                   (artist_id, album_id, track, track_normalized, ts, ts_iso, source)
                   VALUES (?,?,?,?,?,?,?)""",
                (artist_id, album_id, s["track"], track_norm, s["ts"], s["ts_iso"], source)
            )
            stats["new"] += 1
        except sqlite3.IntegrityError:
            stats["dup_exact"] += 1

    return stats

def _commit_stats(conn, total_stats: dict, partial_stats: dict):
    """Acumula stats parciales en total_stats y commitea."""
    conn.commit()
    for k in total_stats:
        total_stats[k] += partial_stats.get(k, 0)

# ─────────────────────────────────────────────
#  LAST.FM
# ─────────────────────────────────────────────
def lfm_get(method, max_retries=6, **params):
    p = {"method": method, "api_key": LASTFM_API_KEY, "format": "json", **params}
    for attempt in range(max_retries):
        try:
            r = requests.get(LASTFM_BASE, params=p, timeout=30)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 10))
                print(f"\n  LFM: rate limit, esperando {wait}s...", end="", flush=True)
                time.sleep(wait + 1)
                continue
            if r.status_code in (500, 502, 503, 504):
                wait = 5 * (2 ** attempt)
                print(f"\n  LFM: HTTP {r.status_code}, reintento {attempt+1}/{max_retries} en {wait}s...",
                      end="", flush=True)
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.SSLError) as exc:
            if attempt >= max_retries - 1:
                raise
            wait = 5 * (2 ** attempt)
            print(f"\n  LFM: error de red ({exc.__class__.__name__}), "
                  f"reintento {attempt+1}/{max_retries} en {wait}s...", end="", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"LFM: máximo de reintentos alcanzado para {method}")

def fetch_lastfm(conn, username: str, from_ts: int = 0) -> dict:
    """
    Descarga scrobbles de Last.fm guardando directamente en la BD cada SAVE_EVERY páginas.
    Reanuda desde el cursor guardado si la ejecución anterior se interrumpió.
    """
    total_stats = {"new": 0, "dup_exact": 0, "dup_cross": 0, "no_artist": 0}

    # ── Cursor de reanudación ──────────────────────────────────────────────
    saved_from = state_get(conn, "lfm_from_ts")
    saved_page = state_get(conn, "lfm_page_cursor")

    if saved_from is not None and int(saved_from) == from_ts and saved_page is not None:
        start_page = int(saved_page)
        print(f"  ⏩ Reanudando Last.fm desde página {start_page} "
              f"(sesión anterior interrumpida)")
    else:
        start_page = 1
        state_set(conn, "lfm_from_ts", from_ts)
        state_set(conn, "lfm_page_cursor", 1)

    # ── Primera petición para conocer total de páginas ─────────────────────
    params: dict = {"user": username, "limit": 200, "page": start_page, "extended": 0}
    if from_ts:
        params["from"] = from_ts
    data = lfm_get("user.getRecentTracks", **params)
    tracks_meta = data.get("recenttracks", {})
    attr = tracks_meta.get("@attr", {})
    total_pages  = int(attr.get("totalPages", 1))
    total_remote = int(attr.get("total", 0))
    print(f"  → {total_remote:,} scrobbles en {total_pages} páginas "
          f"(iniciando en página {start_page})")

    pending = []
    pages_since_save = 0

    def _flush(force=False):
        nonlocal pending, pages_since_save
        if not pending:
            return
        if force or pages_since_save >= SAVE_EVERY:
            st = save_scrobbles(conn, pending, source="lastfm")
            _commit_stats(conn, total_stats, st)
            pending = []
            pages_since_save = 0

    def _process_page(track_list, page_num):
        nonlocal pages_since_save
        for t in track_list:
            if t.get("@attr", {}).get("nowplaying"):
                continue
            di = t.get("date")
            if not di:
                continue
            ts = int(di.get("uts", 0))
            pending.append({
                "artist": t.get("artist", {}).get("#text", "").strip(),
                "album":  t.get("album",  {}).get("#text", "").strip(),
                "track":  t.get("name", "").strip(),
                "ts":     ts,
                "ts_iso": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            })
        pages_since_save += 1
        state_set(conn, "lfm_page_cursor", page_num + 1)

    # Procesar la primera página ya descargada
    track_list = tracks_meta.get("track", [])
    if isinstance(track_list, dict):
        track_list = [track_list]
    _process_page(track_list, start_page)
    print(f"  Página {start_page}/{total_pages}", end="\r")

    # ── Resto de páginas ───────────────────────────────────────────────────
    for page in range(start_page + 1, total_pages + 1):
        _flush()
        params["page"] = page
        data = lfm_get("user.getRecentTracks", **params)
        track_list = data.get("recenttracks", {}).get("track", [])
        if isinstance(track_list, dict):
            track_list = [track_list]
        _process_page(track_list, page)
        print(f"  Página {page}/{total_pages}  (nuevos acum.: {total_stats['new']:,})", end="\r")
        time.sleep(0.25)

    _flush(force=True)

    # ── Limpiar cursor al terminar correctamente ───────────────────────────
    state_del(conn, "lfm_from_ts", "lfm_page_cursor")
    print(f"\n  ✓ Last.fm completo")
    return total_stats

def fetch_lastfm_tags(artist):
    try:
        data = lfm_get("artist.getTopTags", artist=artist, autocorrect=1)
        tags = data.get("toptags", {}).get("tag", [])
        res  = []
        for t in tags[:5]:
            n, c = t.get("name", "").strip().lower(), int(t.get("count", 0))
            if c < 10:
                break
            if n not in GENRE_BLACKLIST and len(n) > 1:
                res.append(n.title())
        return res[:3]
    except Exception:
        return []

# ─────────────────────────────────────────────
#  LISTENBRAINZ
# ─────────────────────────────────────────────
def lb_get(endpoint: str, token: str = "", _attempt: int = 0, **params) -> dict:
    """
    GET a ListenBrainz con reintentos automáticos.
    Backoff exponencial: 5s, 10s, 20s, 40s, 80s, 160s (máx 6 intentos).
    """
    MAX_RETRIES = 6
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Token {token}"

    try:
        r = requests.get(
            LB_BASE + endpoint,
            params=params,
            headers=headers,
            timeout=60
        )
    except (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.SSLError,
        requests.exceptions.ChunkedEncodingError,
    ) as exc:
        if _attempt >= MAX_RETRIES:
            raise
        wait = 5 * (2 ** _attempt)
        print(f"\n  LB: error de red ({exc.__class__.__name__}), "
              f"reintento {_attempt+1}/{MAX_RETRIES} en {wait}s...", end="", flush=True)
        time.sleep(wait)
        return lb_get(endpoint, token, _attempt=_attempt + 1, **params)

    if r.status_code == 429:
        retry = int(r.headers.get("X-RateLimit-Reset-In",
                    r.headers.get("Retry-After", 10)))
        print(f"\n  LB: rate limit, esperando {retry}s...", end="", flush=True)
        time.sleep(retry + 1)
        return lb_get(endpoint, token, _attempt=_attempt, **params)

    if r.status_code in (500, 502, 503, 504):
        if _attempt >= MAX_RETRIES:
            r.raise_for_status()
        wait = 5 * (2 ** _attempt)
        print(f"\n  LB: HTTP {r.status_code}, reintento {_attempt+1}/{MAX_RETRIES} "
              f"en {wait}s...", end="", flush=True)
        time.sleep(wait)
        return lb_get(endpoint, token, _attempt=_attempt + 1, **params)

    r.raise_for_status()
    return r.json()

def fetch_listenbrainz(conn, username: str, from_ts: int = 0) -> dict:
    """
    Descarga todos los listens de ListenBrainz paginando de más reciente a más antiguo.
    Guarda cada lote directamente en la BD y persiste el cursor para poder reanudar.

    Lógica de reanudación:
      - Si hay un cursor lb_max_ts_cursor guardado, continúa desde ahí (sesión interrumpida).
      - Si lb_complete=1, solo descarga listens nuevos (> last_ts_for_source).
      - Si nada: descarga completa desde el más reciente hacia atrás.
    """
    total_stats = {"new": 0, "dup_exact": 0, "dup_cross": 0, "no_artist": 0}
    batch_size  = 100

    lb_complete = state_get(conn, "lb_complete")
    lb_cursor   = state_get(conn, "lb_max_ts_cursor")

    if lb_cursor is not None:
        # Sesión anterior interrumpida — reanudar desde el cursor
        max_ts = int(lb_cursor)
        print(f"  ⏩ Reanudando ListenBrainz desde cursor "
              f"{datetime.fromtimestamp(max_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} UTC")
        # El from_ts de corte también se recupera
        saved_from = state_get(conn, "lb_from_ts")
        effective_from = int(saved_from) if saved_from is not None else from_ts
    elif lb_complete == "1":
        # Descarga incremental: solo novedades
        max_ts = None
        effective_from = last_ts_for_source(conn, "listenbrainz")
        if effective_from:
            dt = datetime.fromtimestamp(effective_from, tz=timezone.utc)
            print(f"  ⏩ Incremental LB desde {dt.strftime('%Y-%m-%d %H:%M')} UTC")
        else:
            print("  Primera ejecución LB, descarga completa")
    else:
        # Primera vez (o primer intento fallido sin cursor guardado)
        max_ts = None
        effective_from = from_ts
        state_set(conn, "lb_from_ts", from_ts)

    print(f"  Descargando de ListenBrainz '{username}'...")
    total_seen  = 0
    batch_count = 0

    while True:
        params: dict = {"count": batch_size}
        if max_ts is not None:
            params["max_ts"] = max_ts

        try:
            data = lb_get(f"user/{username}/listens", token=LB_TOKEN, **params)
        except requests.HTTPError as e:
            if e.response.status_code == 404:
                print(f"\n  ❌ Usuario '{username}' no encontrado en ListenBrainz.")
            else:
                print(f"\n  ❌ Error ListenBrainz HTTP {e.response.status_code}: {e}")
            break

        listens = data.get("payload", {}).get("listens", [])
        if not listens:
            break

        batch: list[dict] = []
        stop = False

        for listen in listens:
            ts = listen.get("listened_at", 0)
            if not ts:
                continue
            if effective_from and ts <= effective_from:
                stop = True
                break
            meta = listen.get("track_metadata", {})
            batch.append({
                "artist": meta.get("artist_name", "").strip(),
                "album":  meta.get("release_name", "").strip(),
                "track":  meta.get("track_name", "").strip(),
                "ts":     ts,
                "ts_iso": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
            })

        if batch:
            st = save_scrobbles(conn, batch, source="listenbrainz")
            conn.commit()
            _commit_stats(conn, total_stats, st)

        total_seen  += len(listens)
        batch_count += 1
        oldest_ts    = listens[-1]["listened_at"]
        max_ts       = oldest_ts - 1

        # Persistir cursor para posible reanudación
        state_set(conn, "lb_max_ts_cursor", max_ts)

        print(f"  {total_seen:,} procesados | nuevos: {total_stats['new']:,} | "
              f"hasta {datetime.fromtimestamp(oldest_ts, tz=timezone.utc).strftime('%Y-%m-%d')}",
              end="\r")

        if stop:
            print(f"\n  ✓ Alcanzado el ts de corte")
            break

        if len(listens) < batch_size:
            break  # última página

        time.sleep(0.2)

    # Limpiar cursores al terminar correctamente
    state_del(conn, "lb_max_ts_cursor", "lb_from_ts")
    state_set(conn, "lb_complete", "1")

    print(f"\n  ✓ ListenBrainz completo — {total_seen:,} procesados")
    return total_stats

# ─────────────────────────────────────────────
#  MUSICBRAINZ
# ─────────────────────────────────────────────
_mb_last = 0.0

def mb_get(endpoint, **params):
    global _mb_last
    w = MB_RATE_LIMIT - (time.time() - _mb_last)
    if w > 0:
        time.sleep(w)
    _mb_last = time.time()
    r = requests.get(
        MB_BASE + endpoint,
        params={**params, "fmt": "json"},
        headers={"User-Agent": MB_UA},
        timeout=30
    )
    if r.status_code == 503:
        time.sleep(5)
        return mb_get(endpoint, **params)
    r.raise_for_status()
    return r.json()

def fetch_mb_tags(artist):
    try:
        data = mb_get("artist", query=f'artist:"{artist}"', limit=1)
        ar = data.get("artists", [])
        if not ar:
            return []
        tags = sorted(ar[0].get("tags", []), key=lambda t: t.get("count", 0), reverse=True)
        return [t["name"].title() for t in tags[:5]
                if t.get("name", "").lower() not in GENRE_BLACKLIST
                and len(t.get("name", "")) > 1][:3]
    except Exception:
        return []

def enrich_genres(conn, source="both"):
    artists = conn.execute(
        "SELECT artist_id,name FROM artists WHERE genres_fetched=0 ORDER BY artist_id"
    ).fetchall()
    print(f"\n  Géneros para {len(artists)} artistas ({source})...")
    for i, (aid, name) in enumerate(artists, 1):
        gs = []
        if source in ("lastfm", "both") and LASTFM_API_KEY:
            gs = fetch_lastfm_tags(name)
        if not gs and source in ("musicbrainz", "both"):
            gs = fetch_mb_tags(name)
        for g in gs:
            link_artist_genre(conn, aid, get_or_create_genre(conn, g))
        conn.execute("UPDATE artists SET genres_fetched=1 WHERE artist_id=?", (aid,))
        if i % 10 == 0 or i == len(artists):
            conn.commit()
            print(f"  {i}/{len(artists)}", end="\r")
    conn.commit()
    print(f"\n  ✓ Géneros listos")

# ─────────────────────────────────────────────
#  ÚLTIMA TS POR FUENTE
# ─────────────────────────────────────────────
def last_ts_for_source(conn, source: str) -> int:
    row = conn.execute(
        "SELECT MAX(ts) FROM scrobbles WHERE source=?", (source,)
    ).fetchone()
    return row[0] or 0

# ─────────────────────────────────────────────
#  MATRICES HORARIAS
# ─────────────────────────────────────────────
def _build_matrix_for_entities(conn, entity_rows):
    labels, totals, rows, peak_hours = [], [], [], []
    for eid, name, total in entity_rows:
        labels.append(name)
        totals.append(total)
        hourly = {h: n for h, n in conn.execute(
            "SELECT CAST(strftime('%H',ts_iso) AS INT),COUNT(*) "
            "FROM scrobbles WHERE artist_id=? GROUP BY 1", (eid,))}
        row = [hourly.get(h, 0) for h in range(24)]
        rows.append(row)
        peak_hours.append(row.index(max(row)) if any(row) else 0)
    return {"labels": labels, "totals": totals, "rows": rows, "peak_hours": peak_hours}

def _build_album_matrix(conn, n):
    top = conn.execute("""
        SELECT al.album_id, ar.name||' — '||al.name, COUNT(*) c
        FROM scrobbles s
        JOIN albums  al ON al.album_id  = s.album_id
        JOIN artists ar ON ar.artist_id = s.artist_id
        WHERE s.album_id IS NOT NULL
        GROUP BY s.album_id ORDER BY c DESC LIMIT ?
    """, (n,)).fetchall()
    labels, totals, rows, peak_hours = [], [], [], []
    for bid, name, total in top:
        labels.append(name); totals.append(total)
        hourly = {h: c for h, c in conn.execute(
            "SELECT CAST(strftime('%H',ts_iso) AS INT),COUNT(*) "
            "FROM scrobbles WHERE album_id=? GROUP BY 1", (bid,))}
        row = [hourly.get(h, 0) for h in range(24)]
        rows.append(row)
        peak_hours.append(row.index(max(row)) if any(row) else 0)
    return {"labels": labels, "totals": totals, "rows": rows, "peak_hours": peak_hours}

def _build_genre_matrix(conn, n):
    top = conn.execute("""
        SELECT g.genre_id, g.name, COUNT(*) c
        FROM scrobbles s
        JOIN artist_genres ag ON ag.artist_id = s.artist_id
        JOIN genres g ON g.genre_id = ag.genre_id
        GROUP BY g.genre_id ORDER BY c DESC LIMIT ?
    """, (n,)).fetchall()
    labels, totals, rows, peak_hours = [], [], [], []
    for gid, name, total in top:
        labels.append(name); totals.append(total)
        hourly = {h: c for h, c in conn.execute("""
            SELECT CAST(strftime('%H',s.ts_iso) AS INT), COUNT(*)
            FROM scrobbles s
            JOIN artist_genres ag ON ag.artist_id=s.artist_id
            WHERE ag.genre_id=? GROUP BY 1
        """, (gid,))}
        row = [hourly.get(h, 0) for h in range(24)]
        rows.append(row)
        peak_hours.append(row.index(max(row)) if any(row) else 0)
    return {"labels": labels, "totals": totals, "rows": rows, "peak_hours": peak_hours}

def build_hourly_matrix(conn):
    print("  Construyendo matriz horaria...")
    top_artists = conn.execute("""
        SELECT ar.artist_id, ar.name, COUNT(*) c FROM scrobbles s
        JOIN artists ar ON ar.artist_id=s.artist_id
        GROUP BY ar.artist_id ORDER BY c DESC LIMIT ?
    """, (MATRIX_TOP_ARTISTS,)).fetchall()
    artist_mx = _build_matrix_for_entities(conn, top_artists)
    album_mx  = _build_album_matrix(conn, MATRIX_TOP_ALBUMS)
    genre_mx  = _build_genre_matrix(conn, MATRIX_TOP_GENRES)

    hour_rankings = {"artists": [], "albums": [], "genres": []}
    for h in range(24):
        for key, mx in [("artists", artist_mx), ("albums", album_mx), ("genres", genre_mx)]:
            ranked = sorted(
                [(mx["labels"][i], mx["rows"][i][h]) for i in range(len(mx["labels"]))],
                key=lambda x: x[1], reverse=True
            )[:15]
            hour_rankings[key].append([{"name": r[0], "n": r[1]} for r in ranked if r[1] > 0])

    return {"artists": artist_mx, "albums": album_mx, "genres": genre_mx,
            "hour_rankings": hour_rankings}

# ─────────────────────────────────────────────
#  EXPORTACIÓN PRINCIPAL
# ─────────────────────────────────────────────
def export_json(conn, path, username_display=""):
    ac = conn.execute("""
        SELECT ar.artist_id, ar.name                               AS artist,
               GROUP_CONCAT(DISTINCT g.name)                       AS genres,
               COUNT(s.scrobble_id)                                AS total_scrobbles,
               MIN(s.ts_iso) AS first_scrobble, MAX(s.ts_iso) AS last_scrobble,
               COUNT(DISTINCT DATE(s.ts_iso))                      AS active_days_span,
               ROUND(CAST(COUNT(s.scrobble_id) AS REAL) /
                     COUNT(DISTINCT DATE(s.ts_iso)), 1)            AS avg_days_between
        FROM artists ar JOIN scrobbles s ON s.artist_id=ar.artist_id
        LEFT JOIN artist_genres ag ON ag.artist_id=ar.artist_id
        LEFT JOIN genres g ON g.genre_id=ag.genre_id
        GROUP BY ar.artist_id ORDER BY total_scrobbles DESC
    """)
    artists = [dict(zip([d[0] for d in ac.description], row)) for row in ac.fetchall()]

    alc = conn.execute("""
        SELECT al.album_id, al.artist_id, ar.name AS artist, al.name AS album,
               GROUP_CONCAT(DISTINCT g.name) AS genres,
               COUNT(s.scrobble_id) AS total_scrobbles,
               MIN(s.ts_iso) AS first_scrobble, MAX(s.ts_iso) AS last_scrobble,
               CAST((MAX(s.ts)-MIN(s.ts))/86400.0 AS INTEGER) AS active_days_span,
               COUNT(DISTINCT DATE(s.ts_iso)) AS days_listened
        FROM albums al JOIN artists ar ON ar.artist_id=al.artist_id
        JOIN scrobbles s ON s.album_id=al.album_id
        LEFT JOIN artist_genres ag ON ag.artist_id=ar.artist_id
        LEFT JOIN genres g ON g.genre_id=ag.genre_id
        GROUP BY al.album_id ORDER BY total_scrobbles DESC
    """)
    albums = [dict(zip([d[0] for d in alc.description], row)) for row in alc.fetchall()]

    monthly = [{"month": r[0], "scrobbles": r[1]} for r in conn.execute(
        "SELECT substr(ts_iso,1,7),COUNT(*) FROM scrobbles GROUP BY 1 ORDER BY 1")]
    hourly  = [{"hour": r[0], "scrobbles": r[1]} for r in conn.execute(
        "SELECT CAST(strftime('%H',ts_iso) AS INT),COUNT(*) FROM scrobbles GROUP BY 1 ORDER BY 1")]
    wd_raw  = {r[0]: r[1] for r in conn.execute(
        "SELECT CAST(strftime('%w',ts_iso) AS INT),COUNT(*) FROM scrobbles GROUP BY 1")}
    weekday = [{"day": d, "scrobbles": wd_raw.get(i, 0)}
               for i, d in enumerate(["Dom","Lun","Mar","Mié","Jue","Vie","Sáb"])]
    genres  = [{"genre": r[0], "artists": r[1], "scrobbles": r[2]} for r in conn.execute("""
        SELECT g.name,COUNT(DISTINCT ag.artist_id),COUNT(s.scrobble_id)
        FROM genres g JOIN artist_genres ag ON ag.genre_id=g.genre_id
        JOIN scrobbles s ON s.artist_id=ag.artist_id
        GROUP BY g.genre_id ORDER BY 3 DESC LIMIT 40
    """)]
    totals  = conn.execute("SELECT COUNT(*),MIN(ts_iso),MAX(ts_iso) FROM scrobbles").fetchone()
    sources = [{"source": r[0], "count": r[1]} for r in conn.execute(
        "SELECT source, COUNT(*) FROM scrobbles GROUP BY source ORDER BY 2 DESC")]

    hourly_matrix = build_hourly_matrix(conn)

    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at":    datetime.now().isoformat(),
            "username":        username_display,
            "sources":         sources,
            "total_scrobbles": totals[0],
            "first_scrobble":  totals[1],
            "last_scrobble":   totals[2],
            "artists":  artists, "albums": albums,
            "monthly":  monthly, "hourly": hourly,
            "weekday":  weekday, "genres": genres,
            "hourly_matrix": hourly_matrix,
        }, f, ensure_ascii=False, indent=2)

    src_str = "  ".join(f"{s['source']}: {s['count']:,}" for s in sources)
    print(f"  → {path}  ({len(artists):,} artistas · {src_str})")

# ─────────────────────────────────────────────
#  EXPORTACIÓN DE DETALLE
# ─────────────────────────────────────────────
def export_detail_json(conn, path):
    print("  Construyendo datos de detalle...")
    ad: dict = {}
    for aid, month, cnt in conn.execute(
        "SELECT artist_id,substr(ts_iso,1,7),COUNT(*) FROM scrobbles GROUP BY 1,2 ORDER BY 1,2"):
        ad.setdefault(aid, {"monthly": [], "top_tracks": [], "top_albums": [],
                            "monthly_albums": {}, "hourly": [0]*24})
        ad[aid]["monthly"].append({"m": month, "n": cnt})
    prev, count = None, 0
    for aid, track, cnt in conn.execute(
        "SELECT artist_id,track,COUNT(*) c FROM scrobbles GROUP BY 1,2 ORDER BY 1,c DESC"):
        if aid != prev: prev, count = aid, 0
        if count >= 20: continue
        ad.setdefault(aid, {"monthly": [], "top_tracks": [], "top_albums": [],
                            "monthly_albums": {}, "hourly": [0]*24})
        ad[aid]["top_tracks"].append({"t": track, "n": cnt}); count += 1
    prev, count = None, 0
    for aid, album, cnt in conn.execute("""
        SELECT s.artist_id,al.name,COUNT(*) c FROM scrobbles s
        JOIN albums al ON al.album_id=s.album_id WHERE s.album_id IS NOT NULL
        GROUP BY 1,s.album_id ORDER BY 1,c DESC"""):
        if aid != prev: prev, count = aid, 0
        if count >= 10: continue
        ad.setdefault(aid, {"monthly": [], "top_tracks": [], "top_albums": [],
                            "monthly_albums": {}, "hourly": [0]*24})
        ad[aid]["top_albums"].append({"a": album, "n": cnt}); count += 1

    # Top 5 álbumes por mes para cada artista
    prev_am, count = None, 0
    for aid, month, album, cnt in conn.execute("""
        SELECT s.artist_id, substr(s.ts_iso,1,7), al.name, COUNT(*) c
        FROM scrobbles s JOIN albums al ON al.album_id=s.album_id
        WHERE s.album_id IS NOT NULL
        GROUP BY 1, 2, s.album_id ORDER BY 1, 2, c DESC
    """):
        key = (aid, month)
        if key != prev_am: prev_am, count = key, 0
        if count >= 5: continue
        ad.setdefault(aid, {"monthly": [], "top_tracks": [], "top_albums": [],
                            "monthly_albums": {}, "hourly": [0]*24})
        ad[aid]["monthly_albums"].setdefault(month, [])
        ad[aid]["monthly_albums"][month].append({"al": album, "n": cnt})
        count += 1

    # Distribución horaria por artista
    for aid, hour, cnt in conn.execute(
        "SELECT artist_id, CAST(strftime('%H',ts_iso) AS INT), COUNT(*) "
        "FROM scrobbles GROUP BY 1,2"):
        if aid in ad:
            ad[aid]["hourly"][hour] = cnt

    ald: dict = {}
    for bid, month, cnt in conn.execute(
        "SELECT album_id,substr(ts_iso,1,7),COUNT(*) FROM scrobbles "
        "WHERE album_id IS NOT NULL GROUP BY 1,2 ORDER BY 1,2"):
        ald.setdefault(bid, {"monthly": [], "top_tracks": [], "monthly_tracks": {}, "hourly": [0]*24})
        ald[bid]["monthly"].append({"m": month, "n": cnt})
    prev, count = None, 0
    for bid, track, cnt in conn.execute(
        "SELECT album_id,track,COUNT(*) c FROM scrobbles WHERE album_id IS NOT NULL "
        "GROUP BY 1,2 ORDER BY 1,c DESC"):
        if bid != prev: prev, count = bid, 0
        if count >= 20: continue
        ald.setdefault(bid, {"monthly": [], "top_tracks": [], "monthly_tracks": {}, "hourly": [0]*24})
        ald[bid]["top_tracks"].append({"t": track, "n": cnt}); count += 1

    # Top 5 pistas por mes para cada álbum
    prev_bm, count = None, 0
    for bid, month, track, cnt in conn.execute("""
        SELECT album_id, substr(ts_iso,1,7), track, COUNT(*) c
        FROM scrobbles WHERE album_id IS NOT NULL
        GROUP BY 1, 2, track ORDER BY 1, 2, c DESC
    """):
        key = (bid, month)
        if key != prev_bm: prev_bm, count = key, 0
        if count >= 5: continue
        ald.setdefault(bid, {"monthly": [], "top_tracks": [], "monthly_tracks": {}, "hourly": [0]*24})
        ald[bid]["monthly_tracks"].setdefault(month, [])
        ald[bid]["monthly_tracks"][month].append({"t": track, "n": cnt})
        count += 1

    # Distribución horaria por álbum
    for bid, hour, cnt in conn.execute(
        "SELECT album_id, CAST(strftime('%H',ts_iso) AS INT), COUNT(*) "
        "FROM scrobbles WHERE album_id IS NOT NULL GROUP BY 1,2"):
        if bid in ald:
            ald[bid]["hourly"][hour] = cnt

    gd: dict = {}
    for gname, month, cnt in conn.execute("""
        SELECT g.name,substr(s.ts_iso,1,7),COUNT(*) FROM scrobbles s
        JOIN artist_genres ag ON ag.artist_id=s.artist_id
        JOIN genres g ON g.genre_id=ag.genre_id GROUP BY 1,2 ORDER BY 1,2"""):
        gd.setdefault(gname, {"monthly": [], "top_artists": [], "monthly_artists": {}, "hourly": [0]*24})
        gd[gname]["monthly"].append({"m": month, "n": cnt})
    prev, count = None, 0
    for gname, artist, cnt in conn.execute("""
        SELECT g.name,ar.name,COUNT(*) c FROM scrobbles s
        JOIN artist_genres ag ON ag.artist_id=s.artist_id
        JOIN genres g ON g.genre_id=ag.genre_id
        JOIN artists ar ON ar.artist_id=s.artist_id
        GROUP BY 1,s.artist_id ORDER BY 1,c DESC"""):
        if gname != prev: prev, count = gname, 0
        if count >= 15: continue
        gd.setdefault(gname, {"monthly": [], "top_artists": [], "monthly_artists": {}, "hourly": [0]*24})
        gd[gname]["top_artists"].append({"a": artist, "n": cnt}); count += 1

    # Top 5 artistas por mes para cada género
    prev_gm, count = None, 0
    for gname, month, artist, cnt in conn.execute("""
        SELECT g.name, substr(s.ts_iso,1,7), ar.name, COUNT(*) c
        FROM scrobbles s
        JOIN artist_genres ag ON ag.artist_id=s.artist_id
        JOIN genres g ON g.genre_id=ag.genre_id
        JOIN artists ar ON ar.artist_id=s.artist_id
        GROUP BY 1, 2, s.artist_id
        ORDER BY 1, 2, c DESC
    """):
        key = (gname, month)
        if key != prev_gm:
            prev_gm = key
            count = 0
        if count >= 5:
            continue
        gd.setdefault(gname, {"monthly": [], "top_artists": [], "monthly_artists": {}, "hourly": [0]*24})
        gd[gname]["monthly_artists"].setdefault(month, [])
        gd[gname]["monthly_artists"][month].append({"a": artist, "n": cnt})
        count += 1

    # Distribución horaria por género
    for gname, hour, cnt in conn.execute("""
        SELECT g.name, CAST(strftime('%H',s.ts_iso) AS INT), COUNT(*)
        FROM scrobbles s
        JOIN artist_genres ag ON ag.artist_id=s.artist_id
        JOIN genres g ON g.genre_id=ag.genre_id
        GROUP BY 1, 2
    """):
        if gname in gd:
            gd[gname]["hourly"][hour] = cnt

    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "artists": {str(k): v for k, v in ad.items()},
            "albums":  {str(k): v for k, v in ald.items()},
            "genres":  gd,
        }, f, ensure_ascii=False, separators=(",", ":"))
    print(f"  → {path}  (~{os.path.getsize(path) // 1024} KB)")

# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────
def main():
    print("🎵 Scrobble Extractor — Last.fm + ListenBrainz")
    print("=" * 50)

    has_lfm = bool(LASTFM_API_KEY and LASTFM_USERNAME)
    has_lb  = bool(LB_USERNAME)

    if not has_lfm and not has_lb:
        print("❌ Configura al menos uno de los servicios en .env:")
        print("   Last.fm:       LASTFM_API_KEY + LASTFM_USERNAME")
        print("   ListenBrainz:  LB_USERNAME  (+ LB_TOKEN opcional)")
        return

    print(f"\n  Last.fm:       {'✓ ' + LASTFM_USERNAME if has_lfm else '— no configurado'}")
    print(f"  ListenBrainz:  {'✓ ' + LB_USERNAME     if has_lb  else '— no configurado'}")
    print(f"  Ventana dedup: ±{DEDUP_WINDOW_SECS}s entre servicios")
    print(f"  Volcado cada:  {SAVE_EVERY} páginas/lotes")

    print(f"\n💾 Abriendo {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")  # más rápido, seguro con WAL
    init_db(conn)

    # ── Last.fm ──────────────────────────────────────────────────────────────
    if has_lfm:
        last_lfm = last_ts_for_source(conn, "lastfm")
        # Si hay cursor guardado, ignoramos last_lfm (la sesión anterior
        # puede tener datos más recientes que los antiguos en la paginación)
        cursor_from = state_get(conn, "lfm_from_ts")
        if cursor_from is not None:
            from_ts_lfm = int(cursor_from)
            print(f"\n📡 Last.fm — reanudando descarga interrumpida")
        elif last_lfm:
            from_ts_lfm = last_lfm + 1
            dt = datetime.fromtimestamp(last_lfm, tz=timezone.utc)
            print(f"\n📡 Last.fm — incremental desde {dt.strftime('%Y-%m-%d %H:%M')} UTC")
        else:
            from_ts_lfm = 0
            print("\n📡 Last.fm — primera ejecución, historial completo")

        print("\n⬇️  Last.fm...")
        s = fetch_lastfm(conn, LASTFM_USERNAME, from_ts=from_ts_lfm)
        print(f"   Nuevos: {s['new']:,}  Dup-exacto: {s['dup_exact']:,}  "
              f"Sin artista: {s['no_artist']:,}")

    # ── ListenBrainz ─────────────────────────────────────────────────────────
    if has_lb:
        lb_cursor  = state_get(conn, "lb_max_ts_cursor")
        lb_complete = state_get(conn, "lb_complete")
        if lb_cursor:
            print(f"\n📡 ListenBrainz — reanudando descarga interrumpida")
        elif lb_complete == "1":
            last_lb = last_ts_for_source(conn, "listenbrainz")
            if last_lb:
                dt = datetime.fromtimestamp(last_lb, tz=timezone.utc)
                print(f"\n📡 ListenBrainz — incremental desde {dt.strftime('%Y-%m-%d %H:%M')} UTC")
            else:
                print("\n📡 ListenBrainz — primera ejecución, historial completo")
        else:
            print("\n📡 ListenBrainz — primera ejecución, historial completo")

        print("\n⬇️  ListenBrainz...")
        s = fetch_listenbrainz(conn, LB_USERNAME,
                               from_ts=last_ts_for_source(conn, "listenbrainz"))
        print(f"   Nuevos: {s['new']:,}  Dup-exacto: {s['dup_exact']:,}  "
              f"Cruzados: {s['dup_cross']:,}  Sin artista: {s['no_artist']:,}")

    # ── Géneros ───────────────────────────────────────────────────────────────
    print("\n🌐 Géneros...")
    enrich_genres(conn, source="both")

    # ── Exportar ──────────────────────────────────────────────────────────────
    username_display = " + ".join(filter(None, [LASTFM_USERNAME, LB_USERNAME]))
    print(f"\n📤 Exportando JSONs...")
    export_json(conn, JSON_PATH, username_display=username_display)
    export_detail_json(conn, DETAIL_JSON_PATH)

    conn.close()
    print("\n✅ ¡Listo! Abre estadisticas_lastfm.html")
    print("   Necesitas en la misma carpeta: lastfm_stats.json + lastfm_detail.json")

if __name__ == "__main__":
    main()
