"""
Información enriquecida de artista/álbum para el panel lateral de dieta_sonora.

Fuentes:
  - Last.fm (api_key en LASTFM_API_KEY): bio, tags, listeners/playcount, similares.
  - MusicBrainz (sin api_key): fecha de lanzamiento, país y sello, como fallback
    para los metadatos que Last.fm no ofrece.

Se usa bajo demanda desde app.py (/api/lastfm_info), con una caché en disco para
no golpear ambas APIs en cada clic del panel.

NOTA: las variables de entorno aquí se leen de forma perezosa (dentro de las
funciones, no como constantes de módulo) porque app.py importa este módulo
ANTES de llamar a load_sops_env() — si se leyeran al importar, siempre
saldrían vacías.
"""

import json
import os
import re
import time

import requests

LASTFM_URL = "https://ws.audioscrobbler.com/2.0/"

MUSICBRAINZ_URL = "https://musicbrainz.org/ws/2"

CACHE_FILE = "lastfm_info_cache.json"  # se puede sobreescribir desde app.py
CACHE_TTL = 7 * 24 * 3600  # 7 días — listeners/playcount cambian pero no hace falta tiempo real


def _mb_headers():
    contact = os.getenv("MB_EMAIL") or "changeme@example.com"
    return {"User-Agent": f"DietaSonoraPanel/1.0 (+{contact})"}


# ──────────────────────────────────────────────────────────────────────────────
# LAST.FM
# ──────────────────────────────────────────────────────────────────────────────

def _lastfm_get(params):
    """
    Petición a la API de Last.fm.
    IMPORTANT: Last.fm siempre responde HTTP 200, incluso en errores.
    Hay que comprobar la clave 'error' en el cuerpo JSON.
    """
    api_key = os.getenv("LASTFM_API_KEY")
    if not api_key:
        return None
    base_params = {
        "api_key": api_key,
        "format": "json",
        "autocorrect": "1",
    }
    base_params.update(params)
    try:
        r = requests.get(LASTFM_URL, params=base_params, timeout=15)
        data = r.json()
    except Exception as e:
        print(f"    ⚠ Last.fm error: {e}")
        return None
    if not isinstance(data, dict) or "error" in data:
        return None
    return data


def _normalize_tags(raw, limit=8):
    """
    Last.fm devuelve las tags con forma inconsistente según cuántas haya:
      0 → {} / [] / ausente · 1 → dict suelto · N → lista de dicts
    """
    if not raw:
        return []
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, dict):
        name = raw.get("name", "")
        return [name] if name else []
    names = []
    for item in raw:
        if isinstance(item, str):
            names.append(item)
        elif isinstance(item, dict) and item.get("name"):
            names.append(item["name"])
    return names[:limit]


def _strip_lastfm_bio(bio_html):
    """Quita el aviso legal final ('Read more on Last.fm') y las etiquetas HTML de la bio."""
    if not bio_html:
        return ""
    text = re.sub(r'<a href="[^"]*">Read more on Last\.fm</a>\.?', "", bio_html)
    text = re.sub(r"<[^>]+>", "", text).strip()
    # Recortar a un párrafo razonable para el panel lateral
    if len(text) > 500:
        text = text[:500].rsplit(" ", 1)[0] + "…"
    return text


def get_artist_info(artist):
    data = _lastfm_get({"method": "artist.getInfo", "artist": artist})
    if not data:
        return {}
    a = data.get("artist")
    if not isinstance(a, dict):
        return {}
    stats = a.get("stats") or {}
    bio = a.get("bio") or {}
    tags_obj = a.get("tags") or {}
    raw_tags = tags_obj.get("tag", []) if isinstance(tags_obj, dict) else []
    return {
        "url": a.get("url", ""),
        "listeners": int(stats.get("listeners") or 0),
        "playcount": int(stats.get("playcount") or 0),
        "tags": _normalize_tags(raw_tags),
        "bio_summary": _strip_lastfm_bio(bio.get("summary", "")),
    }


def get_album_info(artist, album):
    data = _lastfm_get({"method": "album.getInfo", "artist": artist, "album": album})
    if not data:
        return {}
    al = data.get("album")
    if not isinstance(al, dict):
        return {}
    tags_obj = al.get("tags") or {}
    raw_tags = tags_obj.get("tag", []) if isinstance(tags_obj, dict) else []
    return {
        "url": al.get("url", ""),
        "listeners": int(al.get("listeners") or 0),
        "playcount": int(al.get("playcount") or 0),
        "tags": _normalize_tags(raw_tags),
    }


def get_similar_artists(artist, limit=6):
    data = _lastfm_get({"method": "artist.getSimilar", "artist": artist, "limit": limit})
    if not data:
        return []
    raw = (data.get("similarartists") or {}).get("artist", [])
    if isinstance(raw, dict):
        raw = [raw]
    return [
        {"name": a["name"], "url": a.get("url", "")}
        for a in raw
        if isinstance(a, dict) and a.get("name")
    ]


# ──────────────────────────────────────────────────────────────────────────────
# MUSICBRAINZ (fallback de metadatos de lanzamiento)
# ──────────────────────────────────────────────────────────────────────────────

def _mb_get(path, params):
    """Petición a MusicBrainz respetando su límite de 1 req/seg."""
    params = dict(params)
    params["fmt"] = "json"
    try:
        r = requests.get(f"{MUSICBRAINZ_URL}/{path}", params=params, headers=_mb_headers(), timeout=15)
        time.sleep(1.0)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception as e:
        print(f"    ⚠ MusicBrainz error: {e}")
        return None


def get_musicbrainz_release_info(artist, album):
    """Fecha de lanzamiento original, país y sello del primer release que coincida."""
    query = f'artist:"{artist}" AND release:"{album}"'
    data = _mb_get("release", {"query": query, "limit": 1, "inc": "labels"})
    if not data:
        return {}
    releases = data.get("releases", [])
    if not releases:
        return {}
    rel = releases[0]
    label = None
    for li in rel.get("label-info", []) or []:
        label_name = (li.get("label") or {}).get("name")
        if label_name:
            label = label_name
            break
    return {
        "release_date": rel.get("date") or "",
        "country": rel.get("country") or "",
        "label": label or "",
        "mb_url": f"https://musicbrainz.org/release/{rel['id']}" if rel.get("id") else "",
    }


# ──────────────────────────────────────────────────────────────────────────────
# COMBINADO + CACHÉ
# ──────────────────────────────────────────────────────────────────────────────

def get_full_info(artist, album):
    result = {
        "artist": {},
        "album": {},
        "similar": [],
        "sources": {"lastfm": False, "musicbrainz": False},
    }

    if os.getenv("LASTFM_API_KEY"):
        try:
            artist_info = get_artist_info(artist)
            time.sleep(0.25)
            album_info = get_album_info(artist, album)
            time.sleep(0.25)
            similar = get_similar_artists(artist)
            if artist_info or album_info:
                result["sources"]["lastfm"] = True
            result["artist"] = artist_info
            result["album"] = album_info
            result["similar"] = similar
        except Exception as e:
            print(f"    ⚠ Error consultando Last.fm para {artist} – {album}: {e}")

    try:
        mb_info = get_musicbrainz_release_info(artist, album)
        if mb_info:
            result["sources"]["musicbrainz"] = True
            result["album"].update({k: v for k, v in mb_info.items() if v})
    except Exception as e:
        print(f"    ⚠ Error consultando MusicBrainz para {artist} – {album}: {e}")

    return result


def load_cache(cache_file=None):
    path = cache_file or CACHE_FILE
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_cache(cache, cache_file=None):
    path = cache_file or CACHE_FILE
    with open(path, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def get_full_info_cached(artist, album, cache_file=None):
    cache = load_cache(cache_file)
    key = f"{artist}|||{album}"
    entry = cache.get(key)
    now = time.time()

    if entry and (now - entry.get("_fetched_at", 0)) < CACHE_TTL:
        return entry["data"]

    data = get_full_info(artist, album)
    cache[key] = {"_fetched_at": now, "data": data}
    save_cache(cache, cache_file)
    return data
