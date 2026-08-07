import json
import re
import time
import urllib.parse
import urllib.request
import urllib.error
import os

# ──────────────────────────────────────────────────────────────────────────────
# BÚSQUEDA AUTOMÁTICA DE EMBEDS
# ──────────────────────────────────────────────────────────────────────────────

CACHE_FILE = "embeds_cache.json"  # Se puede sobreescribir desde app.py

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _get(url, timeout=10):
    """HTTP GET simple con User-Agent y timeout."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        print(f"      ⚠️  GET error {url}: {e}")
        return ""


def fetch_youtube_embed(artist, album):
    """
    Busca 'artist album full album' en YouTube sin API key.
    Devuelve un iframe HTML listo para insertar, o "" si no encuentra nada.
    """
    query = urllib.parse.quote_plus(f"{artist} {album} full album")
    url = f"https://www.youtube.com/results?search_query={query}"
    html = _get(url)

    # YouTube incrusta los IDs de vídeo en el JSON inicial de la página
    video_ids = re.findall(r'"videoId"\s*:\s*"([A-Za-z0-9_-]{11})"', html)

    # Filtrar IDs duplicados, manteniendo orden
    seen = set()
    unique_ids = []
    for vid in video_ids:
        if vid not in seen:
            seen.add(vid)
            unique_ids.append(vid)

    if not unique_ids:
        return ""

    video_id = unique_ids[0]
    embed_url = f"https://www.youtube.com/embed/{video_id}"
    return (
        f'<iframe width="400" height="160" src="{embed_url}" '
        f'frameborder="0" allow="autoplay; encrypted-media" allowfullscreen></iframe>'
    )


def fetch_bandcamp_embed(artist, album):
    """
    Busca el álbum en Bandcamp search, obtiene la URL del primer resultado
    de tipo 'album', visita la página y extrae el album_id para construir el embed.
    Devuelve un iframe HTML listo para insertar, o "" si no encuentra nada.
    """
    query = urllib.parse.quote_plus(f"{artist} {album}")
    search_url = f"https://bandcamp.com/search?q={query}&item_type=a"
    search_html = _get(search_url)

    # Extraer la primera URL de álbum en los resultados
    album_url_match = re.search(
        r'<div class="result-info">.*?<a href="(https?://[^"]+bandcamp\.com[^"]+)"',
        search_html,
        re.DOTALL,
    )
    if not album_url_match:
        # Fallback: cualquier URL de bandcamp en la página de resultados
        album_url_match = re.search(
            r'href="(https?://[a-z0-9\-]+\.bandcamp\.com/album/[^"]+)"',
            search_html,
        )

    if not album_url_match:
        return ""

    album_url = album_url_match.group(1).split("?")[0]
    print(f"      🔗 Bandcamp URL: {album_url}")

    # Visitar la página del álbum para extraer el album_id
    album_page = _get(album_url)

    album_id = None

    # Método 1: "album_id" explícito — el más fiable, nunca se confunde con track_id
    m = re.search(r'"album_id"\s*:\s*(\d+)', album_page)
    if m:
        album_id = m.group(1)

    # Método 2: TralbumData.current.id cuando item_type es "album"
    if not album_id:
        m = re.search(r'"current"\s*:\s*\{[^}]*"id"\s*:\s*(\d+)', album_page)
        if m:
            item_type = re.search(r'"item_type"\s*:\s*"(\w+)"', album_page)
            if not item_type or item_type.group(1) == "album":
                album_id = m.group(1)

    # Método 3: data-item-id junto a data-item-type="album"
    if not album_id:
        m = re.search(
            r'data-item-type=["\']album["\'][^>]*data-item-id=["\'](\d+)["\']'
            r'|data-item-id=["\'](\d+)["\'][^>]*data-item-type=["\']album["\']',
            album_page,
        )
        if m:
            album_id = m.group(1) or m.group(2)

    # Método 4: EmbeddedPlayer iframe ya presente en la página
    if not album_id:
        m = re.search(r'EmbeddedPlayer/album=(\d+)', album_page)
        if m:
            album_id = m.group(1)

    if not album_id:
        print(f"      ⚠️  No se pudo extraer album_id de {album_url}")
        return ""

    print(f"      ✅ Bandcamp album_id: {album_id}")
    embed_url = (
        f"https://bandcamp.com/EmbeddedPlayer/album={album_id}"
        "/size=large/bgcol=1f1f28/linkcol=35bf88/tracklist=false/artwork=small/transparent=true/"
    )
    return (
        f'<iframe style="border: 0; width: 400px; height: 120px;" '
        f'src="{embed_url}" seamless></iframe>'
    )


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


def enrich_with_embeds(json_data, cache_file=None):
    """
    Recorre json_data, busca embeds de YouTube y Bandcamp para cada álbum
    y los almacena en cache. Añade 'youtube_embed' y 'bandcamp_embed' a cada álbum.
    """
    cache = load_cache(cache_file)
    total = sum(1 for album in json_data for _ in album.get("groups", [None]))
    done = 0

    for album in json_data:
        artist = album["artist"]
        album_name = album["album"]
        cache_key = f"{artist}|||{album_name}"

        done += 1
        print(f"  [{done}/{total}] {artist} – {album_name}")

        if cache_key in cache:
            album["youtube_embed"] = cache[cache_key].get("youtube", "")
            album["bandcamp_embed"] = cache[cache_key].get("bandcamp", "")
            print(f"      📦 Desde caché")
            continue

        # Buscar YouTube
        print(f"      🔍 Buscando en YouTube...")
        yt_embed = fetch_youtube_embed(artist, album_name)
        time.sleep(1.5)

        # Buscar Bandcamp
        print(f"      🔍 Buscando en Bandcamp...")
        bc_embed = fetch_bandcamp_embed(artist, album_name)
        time.sleep(1.5)

        album["youtube_embed"] = yt_embed
        album["bandcamp_embed"] = bc_embed

        cache[cache_key] = {"youtube": yt_embed, "bandcamp": bc_embed}
        save_cache(cache, cache_file)

        if yt_embed:
            print(f"      ✅ YouTube encontrado")
        else:
            print(f"      ❌ YouTube no encontrado")
        if bc_embed:
            print(f"      ✅ Bandcamp encontrado")
        else:
            print(f"      ❌ Bandcamp no encontrado")

    return json_data


# ──────────────────────────────────────────────────────────────────────────────

def generar_html(json_data=None):
    """Genera la shell estática. Los datos se cargan en runtime desde /api/albums."""
    html = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Discos Nuevos</title>
        <link rel="stylesheet" href="/theme-palettes.css">
        <style>
            [data-theme="og"], :root:not([data-theme]) {
                --bg: #0a0e27;
                --surface: #16213e;
                --surface-2: #1f2d4a;
                --border: #2d3e5f;
                --text: #b0b8c9;
                --text-muted: #7a8694;
                --accent: #35bf88;
                --accent-2: #8e44ad;
                --success: #35bf88;
                --warning: #f39c12;
                --danger: #e74c3c;
            }
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: var(--bg);
                color: var(--text);
                display: flex;
                height: 100vh;
                overflow: hidden;
            }
            .main-container {
                display: flex;
                width: 100%;
            }
            .albums-container {
                width: 70%;
                height: 100vh;
                overflow-y: auto;
                padding: 20px;
            }
            .albums-grid {
                display: grid;
                grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
                gap: 20px;
            }
            .album {
                background-color: var(--surface);
                border-radius: 10px;
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.3);
                padding: 10px;
                cursor: pointer;
                transition: transform 0.3s ease, box-shadow 0.3s ease, border-color 0.3s ease;
                text-align: center;
                border: 2px solid var(--surface-2);
                position: relative;
            }
            .album:hover {
                transform: translateY(-5px);
                box-shadow: 0 6px 12px rgba(53, 191, 136, 0.3);
                border-color: var(--accent);
            }
            .album.selected {
                border: 3px solid var(--accent);
                box-shadow: 0 6px 12px rgba(53, 191, 136, 0.5);
            }
            .album img {
                width: 100%;
                border-radius: 5px;
                aspect-ratio: 1;
                object-fit: cover;
            }
            .album-artist {
                margin-top: 10px;
                font-size: 14px;
                font-weight: bold;
                color: #ffffff;
            }
            .album-name {
                font-size: 13px;
                color: var(--text);
                margin-top: 5px;
            }
            .album-date {
                font-size: 12px;
                color: var(--text-muted);
                margin-top: 5px;
            }
            .album-type-badge {
                position: absolute;
                top: 10px;
                left: 10px;
                font-size: 10px;
                font-weight: 700;
                padding: 2px 6px;
                border-radius: 4px;
                letter-spacing: 0.5px;
                text-transform: uppercase;
                opacity: 0.85;
            }
            .badge-top10 { background: var(--warning); color: var(--bg); }
            .badge-manual { background: var(--accent-2); color: #fff; }

            .album.type-top10 { border-color: var(--warning); }
            .album.type-top10:hover { border-color: var(--warning); box-shadow: 0 6px 12px rgba(243,156,18,0.35); }
            .album.type-manual { border-color: var(--accent-2); }
            .album.type-manual:hover { border-color: var(--accent-2); box-shadow: 0 6px 12px rgba(142,68,173,0.35); }

            .delete-btn {
                position: absolute;
                top: 10px;
                right: 10px;
                background: var(--danger);
                color: white;
                border: none;
                border-radius: 50%;
                width: 30px;
                height: 30px;
                cursor: pointer;
                font-size: 18px;
                display: flex;
                align-items: center;
                justify-content: center;
                opacity: 0;
                transition: opacity 0.3s ease, transform 0.2s ease;
                z-index: 10;
            }
            .album:hover .delete-btn {
                opacity: 1;
            }
            .delete-btn:hover {
                transform: scale(1.1);
                background: #c0392b;
            }
            .sidebar {
                width: 30%;
                height: 100vh;
                background-color: var(--surface);
                border-left: 2px solid var(--surface-2);
                overflow-y: auto;
                padding: 20px;
            }
            .sidebar h2 {
                font-size: 18px;
                margin-bottom: 15px;
                color: var(--accent);
            }
            .sidebar-placeholder {
                color: var(--text-muted);
                text-align: center;
                margin-top: 50px;
                font-size: 14px;
            }
            .flac-table {
                width: 100%;
                border-collapse: collapse;
                font-size: 12px;
            }
            .flac-table th {
                background-color: var(--surface-2);
                padding: 8px;
                border: 1px solid var(--border);
                text-align: left;
                position: sticky;
                top: 0;
                font-size: 11px;
                color: var(--accent);
            }
            .flac-table td {
                padding: 8px;
                border: 1px solid var(--border);
                background-color: var(--surface);
                color: var(--text);
            }
            .download-btn {
                background: var(--accent);
                color: var(--bg);
                border: none;
                padding: 5px 10px;
                border-radius: 5px;
                cursor: pointer;
                font-weight: 600;
                transition: transform 0.2s ease, box-shadow 0.2s ease;
                font-size: 11px;
            }
            .download-btn:hover {
                transform: scale(1.05);
                box-shadow: 0 3px 10px rgba(53, 191, 136, 0.5);
            }
            .download-btn:disabled {
                background: var(--text-muted);
                cursor: not-allowed;
            }
            .album-header {
                margin-bottom: 15px;
                padding-bottom: 10px;
                border-bottom: 2px solid var(--surface-2);
            }
            .album-header h3 {
                font-size: 16px;
                color: #ffffff;
                margin-bottom: 5px;
            }
            .album-header p {
                font-size: 12px;
                color: var(--text);
            }
            h1 {
                padding: 20px;
                background-color: var(--surface);
                border-bottom: 2px solid var(--surface-2);
                margin: 0;
                color: var(--accent);
                text-shadow: 0 0 10px rgba(53, 191, 136, 0.3);
            }
            .page-header {
                display: flex;
                align-items: center;
                justify-content: space-between;
                padding: 15px 20px;
                background-color: var(--surface);
                border-bottom: 2px solid var(--surface-2);
            }
            .page-header h1 {
                padding: 0;
                border: none;
                margin: 0;
            }
            .header-actions {
                display: flex;
                gap: 10px;
            }
            .action-btn {
                display: flex;
                align-items: center;
                gap: 7px;
                background-color: var(--surface-2);
                color: var(--text);
                border: 1px solid var(--border);
                padding: 8px 14px;
                border-radius: 8px;
                cursor: pointer;
                font-size: 13px;
                font-weight: 600;
                transition: background-color 0.2s ease, border-color 0.2s ease, color 0.2s ease, box-shadow 0.2s ease;
                white-space: nowrap;
            }
            .action-btn:hover {
                background-color: var(--accent);
                border-color: var(--accent);
                color: var(--bg);
                box-shadow: 0 3px 10px rgba(53, 191, 136, 0.4);
            }
            .action-btn:disabled {
                opacity: 0.5;
                cursor: not-allowed;
                background-color: var(--surface-2);
                color: var(--text-muted);
                border-color: var(--border);
                box-shadow: none;
            }
            .action-btn .btn-icon {
                font-size: 16px;
            }

            .tabs {
                display: flex;
                gap: 8px;
                padding: 12px 20px 0;
                background-color: var(--surface);
                border-bottom: 2px solid var(--surface-2);
            }
            .tab-btn {
                display: flex;
                align-items: center;
                gap: 8px;
                background: transparent;
                color: var(--text-muted);
                border: none;
                border-bottom: 3px solid transparent;
                padding: 10px 6px 12px;
                cursor: pointer;
                font-size: 14px;
                font-weight: 600;
                transition: color 0.2s ease, border-color 0.2s ease;
            }
            .tab-btn:hover {
                color: var(--text);
            }
            .tab-btn.active {
                color: var(--accent);
                border-bottom-color: var(--accent);
            }
            .tab-btn[data-tab="top10"].active {
                color: var(--warning);
                border-bottom-color: var(--warning);
            }
            .tab-count {
                background: var(--surface-2);
                color: var(--text);
                font-size: 11px;
                font-weight: 700;
                padding: 2px 8px;
                border-radius: 10px;
            }

            /* Scrollbar personalizado */
            ::-webkit-scrollbar {
                width: 10px;
            }
            ::-webkit-scrollbar-track {
                background: var(--surface-2);
            }
            ::-webkit-scrollbar-thumb {
                background: var(--accent);
                border-radius: 5px;
            }
            ::-webkit-scrollbar-thumb:hover {
                background: #2da672;
            }

            /* Embeds en sidebar */
            .embeds-section {
                margin-top: 18px;
                padding-top: 14px;
                border-top: 2px solid var(--surface-2);
            }
            .embeds-section h4 {
                font-size: 12px;
                color: var(--accent);
                margin-bottom: 10px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            .embed-block {
                margin-bottom: 12px;
            }
            .embed-label {
                font-size: 11px;
                color: var(--text-muted);
                margin-bottom: 5px;
            }
            .embed-block iframe {
                width: 100% !important;
                border-radius: 6px;
                display: block;
            }

            /* Info Last.fm / MusicBrainz en sidebar (debajo del video) */
            .lastfm-section {
                margin-top: 18px;
                padding-top: 14px;
                border-top: 2px solid var(--surface-2);
            }
            .lastfm-section h4 {
                font-size: 12px;
                color: var(--accent);
                margin-bottom: 10px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            .lastfm-loading {
                color: var(--text-muted);
                font-size: 12px;
                display: flex;
                align-items: center;
                gap: 4px;
            }
            .lastfm-empty {
                color: var(--text-muted);
                font-size: 12px;
                font-style: italic;
            }
            .lastfm-meta {
                font-size: 11px;
                color: var(--text-muted);
                margin-bottom: 10px;
            }
            .lastfm-bio {
                font-size: 12px;
                line-height: 1.5;
                color: var(--text);
                margin-bottom: 12px;
                max-height: 220px;
                overflow-y: auto;
                white-space: pre-line;
            }
            .lastfm-bio a {
                color: var(--accent);
                white-space: nowrap;
            }
            .lastfm-subtitle {
                font-size: 10px;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                color: var(--text-muted);
                margin: 14px 0 6px;
            }
            .lastfm-tracks {
                list-style: none;
                display: flex;
                flex-direction: column;
                gap: 3px;
                margin-bottom: 12px;
                font-size: 11px;
            }
            .lastfm-track {
                display: flex;
                align-items: center;
                gap: 8px;
                color: var(--text);
                padding: 2px 0;
            }
            .lastfm-track-rank {
                color: var(--text-muted);
                width: 18px;
                text-align: right;
                flex-shrink: 0;
            }
            .lastfm-track-name {
                flex: 1;
                overflow: hidden;
                text-overflow: ellipsis;
                white-space: nowrap;
            }
            .lastfm-track-duration {
                color: var(--text-muted);
                flex-shrink: 0;
            }
            .lastfm-discography {
                display: flex;
                flex-wrap: wrap;
                gap: 6px;
                margin-bottom: 12px;
            }
            .lastfm-discography a {
                font-size: 11px;
                color: var(--text);
                text-decoration: none;
                background: var(--surface-2);
                padding: 3px 8px;
                border-radius: 10px;
            }
            .lastfm-discography a:hover {
                background: var(--border);
                color: #ffffff;
            }
            .lastfm-discography a .year {
                color: var(--accent);
                margin-right: 4px;
            }
            .lastfm-stats {
                display: flex;
                gap: 16px;
                margin-bottom: 12px;
                flex-wrap: wrap;
            }
            .lastfm-stat strong {
                display: block;
                font-size: 14px;
                color: #ffffff;
            }
            .lastfm-stat {
                font-size: 10px;
                color: var(--text-muted);
            }
            .lastfm-tags {
                display: flex;
                flex-wrap: wrap;
                gap: 6px;
                margin-bottom: 12px;
            }
            .lastfm-tag {
                background: var(--surface-2);
                color: var(--text);
                font-size: 10px;
                padding: 3px 8px;
                border-radius: 10px;
            }
            .lastfm-similar {
                display: flex;
                flex-wrap: wrap;
                gap: 6px;
            }
            .lastfm-similar a {
                font-size: 11px;
                color: var(--accent);
                text-decoration: none;
                background: rgba(53, 191, 136, 0.1);
                padding: 3px 8px;
                border-radius: 10px;
            }
            .lastfm-similar a:hover {
                background: rgba(53, 191, 136, 0.25);
            }

            /* Loader */
            .loader {
                border: 3px solid var(--surface-2);
                border-top: 3px solid var(--accent);
                border-radius: 50%;
                width: 20px;
                height: 20px;
                animation: spin 1s linear infinite;
                display: inline-block;
                margin-left: 5px;
            }
            @keyframes spin {
                0% { transform: rotate(0deg); }
                100% { transform: rotate(360deg); }
            }

            /* Notificación */
            .notification {
                position: fixed;
                top: 20px;
                right: 20px;
                background: var(--accent);
                color: var(--bg);
                padding: 15px 25px;
                border-radius: 10px;
                box-shadow: 0 5px 20px rgba(53, 191, 136, 0.5);
                z-index: 1000;
                opacity: 0;
                transform: translateY(-20px);
                transition: opacity 0.3s ease, transform 0.3s ease;
            }
            .notification.show {
                opacity: 1;
                transform: translateY(0);
            }
            .notification.error {
                background: var(--danger);
                color: white;
            }
        </style>
    </head>
    <body>
        <div class="main-container">
            <div class="albums-container">
                <div class="page-header">
                    <h1>💿 Discos Nuevos - FLAC</h1>
                    <div class="header-actions">
                        <button class="action-btn" id="btn-airsonic" onclick="ejecutarAccion('airsonic', this)">
                            <span class="btn-icon">🔄</span> Actualizar Airsonic
                        </button>
                        <button class="action-btn" id="btn-calendario" onclick="ejecutarAccion('calendario', this)">
                            <span class="btn-icon">📅</span> Revisar Calendario
                        </button>
                        <button class="action-btn" id="btn-escuchados" onclick="ejecutarAccion('escuchados', this)">
                            <span class="btn-icon">🎧</span> Discos Escuchados
                        </button>
                    </div>
                </div>
                <div class="tabs" id="tabs">
                    <button class="tab-btn active" data-tab="seguidos" onclick="switchTab('seguidos')">
                        🎯 Seguidos <span class="tab-count" id="count-seguidos">0</span>
                    </button>
                    <button class="tab-btn" data-tab="top10" onclick="switchTab('top10')">
                        🏆 Top 10 Orpheus <span class="tab-count" id="count-top10">0</span>
                    </button>
                </div>
                <div class="albums-grid">
                    <p style="color:#7a8694;padding:20px">Cargando álbumes...</p>
                </div>
            </div>
            <div class="sidebar" id="sidebar">
                <div class="sidebar-placeholder">
                    Selecciona un álbum para ver los torrents disponibles
                </div>
            </div>
        </div>

        <div id="notification" class="notification"></div>

        <script>
            let currentSelected = null;
            let currentTab = 'seguidos';
            window.torrentData = {};
            window.embedData = {};
            window.allEntries = [];

            function showNotification(message, isError = false) {
                const notification = document.getElementById('notification');
                notification.textContent = message;
                notification.className = 'notification show' + (isError ? ' error' : '');
                setTimeout(() => { notification.classList.remove('show'); }, 3000);
            }

            async function loadAlbums() {
                const grid = document.querySelector('.albums-grid');
                try {
                    const response = await fetch('/api/albums');
                    if (!response.ok) throw new Error('HTTP ' + response.status);
                    const data = await response.json();

                    window.torrentData = {};
                    window.embedData = {};
                    window.allEntries = [];

                    for (const album of data) {
                        for (const group of album.groups) {
                            const groupId = group.groupId;
                            window.torrentData[groupId] = group.torrents;
                            window.embedData[groupId] = {
                                youtube: album.youtube_embed || '',
                                bandcamp: album.bandcamp_embed || ''
                            };

                            const type = album.type || 'vevent';
                            window.allEntries.push({ album, group, type });
                        }
                    }

                    renderTabCounts();
                    renderGrid();
                } catch (e) {
                    grid.innerHTML = '<p style="color:#e74c3c;padding:20px">Error cargando álbumes: ' + e.message + '</p>';
                }
            }

            function renderTabCounts() {
                const top10Count = window.allEntries.filter(e => e.type === 'top10').length;
                const seguidosCount = window.allEntries.length - top10Count;
                document.getElementById('count-seguidos').textContent = seguidosCount;
                document.getElementById('count-top10').textContent = top10Count;
            }

            function switchTab(tab) {
                currentTab = tab;
                document.querySelectorAll('.tab-btn').forEach(btn => {
                    btn.classList.toggle('active', btn.dataset.tab === tab);
                });
                renderGrid();
            }

            function renderGrid() {
                const grid = document.querySelector('.albums-grid');
                grid.innerHTML = '';

                const entries = window.allEntries.filter(e =>
                    currentTab === 'top10' ? e.type === 'top10' : e.type !== 'top10'
                );

                for (const { album, group, type } of entries) {
                    const groupId = group.groupId;
                    const years = (group.torrents || []).map(t => t.remasterYear).filter(Boolean);
                    const oldestYear = years.length > 0 ? Math.min(...years) : 'Unknown';
                    const flacCount = group.flacCount || 0;

                    const badgeHtml =
                        type === 'top10'  ? '<span class="album-type-badge badge-top10">top10</span>' :
                        type === 'manual' ? '<span class="album-type-badge badge-manual">manual</span>' : '';

                    const div = document.createElement('div');
                    div.className = 'album type-' + type;
                    div.id = 'album-' + groupId;
                    div.onclick = () => showTorrents(groupId, album.artist, album.album, oldestYear, flacCount);
                    div.innerHTML =
                        badgeHtml +
                        '<button class="delete-btn" onclick="deleteAlbum(' + groupId + ', event)" title="Eliminar álbum">×</button>' +
                        '<img src="' + (group.cover || '') + '" alt="' + album.artist + ' - ' + album.album + '">' +
                        '<div class="album-artist">' + album.artist + '</div>' +
                        '<div class="album-name">' + album.album + '</div>' +
                        '<div class="album-date">(' + oldestYear + ') · ' + flacCount + ' FLAC' + (flacCount !== 1 ? 's' : '') + '</div>';
                    grid.appendChild(div);
                }

                if (grid.children.length === 0) {
                    const emptyMsg = currentTab === 'top10'
                        ? 'No hay discos del Top 10 pendientes (o ya los tienes todos).'
                        : 'No hay álbumes pendientes.';
                    grid.innerHTML = '<p style="color:#7a8694;padding:20px">' + emptyMsg + '</p>';
                }
            }

            document.addEventListener('DOMContentLoaded', loadAlbums);

            function showTorrents(groupId, artist, albumName, year, flacCount) {
                // Actualizar selección visual
                if (currentSelected) {
                    document.getElementById('album-' + currentSelected).classList.remove('selected');
                }
                document.getElementById('album-' + groupId).classList.add('selected');
                currentSelected = groupId;

                const torrents = window.torrentData[groupId];
                const sidebar = document.getElementById('sidebar');

                let tableHtml = `
                    <div class="album-header">
                        <h3>${artist}</h3>
                        <p>${albumName} (${year})</p>
                        <p>${flacCount} torrent${flacCount > 1 ? 's' : ''} FLAC disponible${flacCount > 1 ? 's' : ''}</p>
                    </div>
                    <table class="flac-table">
                        <thead>
                            <tr>
                                <th>Media</th>
                                <th>Año</th>
                                <th>Master</th>
                                <th>#</th>
                                <th>MB</th>
                                <th>Dwn</th>
                            </tr>
                        </thead>
                        <tbody>
                `;

                torrents.forEach((torrent, index) => {
                    const media = torrent.media || 'N/A';
                    const remasterYear = torrent.remasterYear || year;
                    const remasterTitle = torrent.remasterTitle || 'Original';
                    const fileCount = torrent.fileCount || 0;
                    const sizeMB = (torrent.size / (1024 * 1024)).toFixed(2);
                    const downloadUrl = torrent.downloadUrl || '#';
                    const torrentId = torrent.id || index;

                    tableHtml += `
                        <tr>
                            <td>${media}</td>
                            <td>${remasterYear}</td>
                            <td>${remasterTitle}</td>
                            <td>${fileCount}</td>
                            <td>${sizeMB}</td>
                            <td>
                                <button class="download-btn" onclick="downloadTorrent('${downloadUrl}', ${groupId}, '${torrentId}')" id="download-${torrentId}">
                                    🢛
                                </button>
                            </td>
                        </tr>
                    `;
                });

                tableHtml += `
                        </tbody>
                    </table>
                `;

                // Embeds de YouTube y Bandcamp
                const embeds = window.embedData ? window.embedData[groupId] : null;
                if (embeds && (embeds.youtube || embeds.bandcamp)) {
                    tableHtml += '<div class="embeds-section"><h4>🎧 Escuchar</h4>';

                    if (embeds.youtube) {
                        tableHtml += '<div class="embed-block">'
                            + '<div class="embed-label">▶️ YouTube</div>'
                            + embeds.youtube
                            + '</div>';
                    }

                    if (embeds.bandcamp) {
                        tableHtml += '<div class="embed-block">'
                            + '<div class="embed-label">🎵 Bandcamp</div>'
                            + embeds.bandcamp
                            + '</div>';
                    }

                    tableHtml += '</div>';
                }

                // Placeholder de info de Last.fm/MusicBrainz — se rellena bajo demanda
                tableHtml += `
                    <div class="lastfm-section" id="lastfm-section">
                        <h4>📻 Sobre el artista / álbum</h4>
                        <div class="lastfm-loading"><span class="loader"></span> Cargando información…</div>
                    </div>
                `;

                sidebar.innerHTML = tableHtml;

                fetchLastfmInfo(artist, albumName, groupId);
            }

            function escapeHtml(str) {
                const div = document.createElement('div');
                div.textContent = str == null ? '' : String(str);
                return div.innerHTML;
            }

            async function fetchLastfmInfo(artist, albumName, groupId) {
                try {
                    const params = new URLSearchParams({ artist, album: albumName });
                    const response = await fetch('/api/lastfm_info?' + params.toString());

                    // Si el usuario ya cambió de álbum mientras esperábamos, no pisar el sidebar
                    if (currentSelected !== groupId) return;

                    const section = document.getElementById('lastfm-section');
                    if (!section) return;

                    if (!response.ok) {
                        section.innerHTML = '<h4>📻 Sobre el artista / álbum</h4><div class="lastfm-empty">No se pudo obtener información.</div>';
                        return;
                    }

                    const data = await response.json();
                    renderLastfmInfo(section, data);
                } catch (error) {
                    if (currentSelected !== groupId) return;
                    const section = document.getElementById('lastfm-section');
                    if (section) {
                        section.innerHTML = '<h4>📻 Sobre el artista / álbum</h4><div class="lastfm-empty">Error al cargar información.</div>';
                    }
                }
            }

            function fmtTrackDuration(seconds) {
                if (!seconds) return '';
                const m = Math.floor(seconds / 60);
                const s = seconds % 60;
                return `${m}:${String(s).padStart(2, '0')}`;
            }

            function renderLastfmInfo(section, data) {
                const artistInfo = data.artist || {};
                const albumInfo = data.album || {};
                const similar = data.similar || [];
                const discography = data.discography || [];
                const sources = data.sources || {};

                if (!sources.lastfm && !sources.musicbrainz) {
                    section.innerHTML = '<h4>📻 Sobre el artista / álbum</h4><div class="lastfm-empty">Sin información disponible.</div>';
                    return;
                }

                let html = '<h4>📻 Sobre el artista / álbum</h4>';

                // Metadatos de lanzamiento (MusicBrainz)
                const metaParts = [];
                if (albumInfo.release_date) metaParts.push(albumInfo.release_date);
                if (albumInfo.label) metaParts.push(albumInfo.label);
                if (albumInfo.country) metaParts.push(albumInfo.country);
                if (metaParts.length) {
                    html += `<div class="lastfm-meta">${escapeHtml(metaParts.join(' · '))}</div>`;
                }

                // Bio completa del artista
                if (artistInfo.bio) {
                    const bioUrl = artistInfo.url || '#';
                    html += `<div class="lastfm-bio">${escapeHtml(artistInfo.bio)} `
                        + `<a href="${encodeURI(bioUrl)}" target="_blank" rel="noopener">Ver en Last.fm →</a></div>`;
                }

                // Oyentes / reproducciones
                const stats = [];
                if (albumInfo.listeners) stats.push({ label: 'Oyentes (álbum)', value: albumInfo.listeners });
                if (albumInfo.playcount) stats.push({ label: 'Reproducciones (álbum)', value: albumInfo.playcount });
                if (artistInfo.listeners) stats.push({ label: 'Oyentes (artista)', value: artistInfo.listeners });
                if (stats.length) {
                    html += '<div class="lastfm-stats">' + stats.map(s =>
                        `<div class="lastfm-stat"><strong>${s.value.toLocaleString('es')}</strong>${escapeHtml(s.label)}</div>`
                    ).join('') + '</div>';
                }

                // Tags/géneros
                const tags = [...new Set([...(albumInfo.tags || []), ...(artistInfo.tags || [])])].slice(0, 8);
                if (tags.length) {
                    html += '<div class="lastfm-tags">' + tags.map(t =>
                        `<span class="lastfm-tag">${escapeHtml(t)}</span>`
                    ).join('') + '</div>';
                }

                // Canciones del álbum
                if (albumInfo.tracks && albumInfo.tracks.length) {
                    html += '<div class="lastfm-subtitle">Canciones</div>';
                    html += '<ul class="lastfm-tracks">' + albumInfo.tracks.map(t => `
                        <li class="lastfm-track">
                            <span class="lastfm-track-rank">${t.rank}.</span>
                            <span class="lastfm-track-name" title="${escapeHtml(t.name)}">${escapeHtml(t.name)}</span>
                            <span class="lastfm-track-duration">${fmtTrackDuration(t.duration)}</span>
                        </li>
                    `).join('') + '</ul>';
                }

                // Discografía (MusicBrainz)
                if (discography.length) {
                    html += '<div class="lastfm-subtitle">Discografía</div>';
                    html += '<div class="lastfm-discography">' + discography.map(d =>
                        `<a href="${encodeURI('https://musicbrainz.org/release-group/' + d.mbid)}" target="_blank" rel="noopener">`
                        + `<span class="year">${escapeHtml(d.year)}</span>${escapeHtml(d.title)}</a>`
                    ).join('') + '</div>';
                }

                // Artistas similares
                if (similar.length) {
                    html += '<div class="lastfm-subtitle">Artistas similares</div>';
                    html += '<div class="lastfm-similar">' + similar.map(s =>
                        `<a href="${encodeURI(s.url || '#')}" target="_blank" rel="noopener">${escapeHtml(s.name)}</a>`
                    ).join('') + '</div>';
                }

                section.innerHTML = html;
            }

            async function downloadTorrent(downloadUrl, groupId, torrentId) {
                const btn = document.getElementById('download-' + torrentId);
                const originalText = btn.textContent;

                btn.disabled = true;
                btn.innerHTML = 'Descargando <span class="loader"></span>';

                try {
                    const response = await fetch('/api/download', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                            downloadUrl: downloadUrl,
                            groupId: groupId,
                            torrentId: torrentId
                        })
                    });

                    const data = await response.json();

                    if (response.ok) {
                        showNotification('✓ Descarga completada. Recargando página...');
                        setTimeout(() => {
                            window.location.reload();
                        }, 2000);
                    } else {
                        showNotification('✗ Error: ' + (data.error || 'Error desconocido'), true);
                        btn.disabled = false;
                        btn.textContent = originalText;
                    }
                } catch (error) {
                    console.error('Error completo:', error);
                    showNotification('✗ Error de conexión: ' + error.message, true);
                    btn.disabled = false;
                    btn.textContent = originalText;
                }
            }

            async function deleteAlbum(groupId, event) {
                event.stopPropagation();

                if (!confirm('¿Estás seguro de que quieres eliminar este álbum?')) {
                    return;
                }

                try {
                    const response = await fetch('/api/delete', {
                        method: 'POST',
                        headers: {
                            'Content-Type': 'application/json',
                        },
                        body: JSON.stringify({
                            groupId: groupId
                        })
                    });

                    const data = await response.json();

                    if (response.ok) {
                        showNotification('✓ Álbum eliminado. Recargando página...');
                        setTimeout(() => {
                            window.location.reload();
                        }, 1500);
                    } else {
                        showNotification('✗ Error: ' + (data.error || 'Error desconocido'), true);
                    }
                } catch (error) {
                    console.error('Error completo:', error);
                    showNotification('✗ Error de conexión: ' + error.message, true);
                }
            }
            async function ejecutarAccion(accion, btn) {
                const textos = {
                    airsonic:    { original: '<span class="btn-icon">🔄</span> Actualizar Airsonic',    loading: '⏳ Actualizando...' },
                    calendario:  { original: '<span class="btn-icon">📅</span> Revisar Calendario',     loading: '⏳ Revisando...' },
                    escuchados:  { original: '<span class="btn-icon">🎧</span> Discos Escuchados',       loading: '⏳ Procesando...' }
                };

                const t = textos[accion];
                btn.disabled = true;
                btn.innerHTML = t.loading;

                try {
                    const response = await fetch('/api/' + accion, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' }
                    });

                    const data = await response.json();

                    if (response.ok) {
                        showNotification('✓ ' + data.message);
                    } else {
                        showNotification('✗ Error: ' + (data.error || 'Error desconocido'), true);
                    }
                } catch (error) {
                    showNotification('✗ Error de conexión: ' + error.message, true);
                } finally {
                    btn.disabled = false;
                    btn.innerHTML = t.original;
                }
            }
        </script>
        <script src="/theme-picker.js"></script>
        <script src="/settings-panel.js"></script>
    </body>
    </html>
    """

    return html


def main():
    # Cargar el archivo JSON con los resultados
    with open("resultado_flacs.json", "r", encoding="utf-8") as f:
        json_data = json.load(f)

    # Buscar embeds de YouTube y Bandcamp para cada álbum (con caché)
    print(f"\n{'='*60}")
    print("🔍 Buscando embeds de YouTube y Bandcamp...")
    print(f"{'='*60}")
    json_data = enrich_with_embeds(json_data)
    print(f"{'='*60}\n")

    html = generar_html(json_data)

    # Guardar el HTML generado
    with open("resumen_flacs.html", "w", encoding="utf-8") as f:
        f.write(html)
    print("✅ HTML generado correctamente → resumen_flacs.html")
    print(f"💾 Caché de embeds guardada → {CACHE_FILE}")


if __name__ == "__main__":
    main()
