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

def generar_html(json_data):
    html = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Discos Nuevos</title>
        <style>
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #0a0e27;
                color: #b0b8c9;
                display: flex;
                height: 100vh;
                overflow: hidden;
            }
            .main-container {
                display: flex;
                width: 100%;
            }
            .albums-container {
                width: 80%;
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
                background-color: #16213e;
                border-radius: 10px;
                box-shadow: 0 4px 8px rgba(0, 0, 0, 0.3);
                padding: 10px;
                cursor: pointer;
                transition: transform 0.3s ease, box-shadow 0.3s ease, border-color 0.3s ease;
                text-align: center;
                border: 2px solid #1f2d4a;
                position: relative;
            }
            .album:hover {
                transform: translateY(-5px);
                box-shadow: 0 6px 12px rgba(53, 191, 136, 0.3);
                border-color: #35bf88;
            }
            .album.selected {
                border: 3px solid #35bf88;
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
                color: #b0b8c9;
                margin-top: 5px;
            }
            .album-date {
                font-size: 12px;
                color: #7a8694;
                margin-top: 5px;
            }
            .delete-btn {
                position: absolute;
                top: 10px;
                right: 10px;
                background: #e74c3c;
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
                width: 20%;
                height: 100vh;
                background-color: #16213e;
                border-left: 2px solid #1f2d4a;
                overflow-y: auto;
                padding: 20px;
            }
            .sidebar h2 {
                font-size: 18px;
                margin-bottom: 15px;
                color: #35bf88;
            }
            .sidebar-placeholder {
                color: #7a8694;
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
                background-color: #1f2d4a;
                padding: 8px;
                border: 1px solid #2d3e5f;
                text-align: left;
                position: sticky;
                top: 0;
                font-size: 11px;
                color: #35bf88;
            }
            .flac-table td {
                padding: 8px;
                border: 1px solid #2d3e5f;
                background-color: #16213e;
                color: #b0b8c9;
            }
            .download-btn {
                background: #35bf88;
                color: #0a0e27;
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
                background: #7a8694;
                cursor: not-allowed;
            }
            .album-header {
                margin-bottom: 15px;
                padding-bottom: 10px;
                border-bottom: 2px solid #1f2d4a;
            }
            .album-header h3 {
                font-size: 16px;
                color: #ffffff;
                margin-bottom: 5px;
            }
            .album-header p {
                font-size: 12px;
                color: #b0b8c9;
            }
            h1 {
                padding: 20px;
                background-color: #16213e;
                border-bottom: 2px solid #1f2d4a;
                margin: 0;
                color: #35bf88;
                text-shadow: 0 0 10px rgba(53, 191, 136, 0.3);
            }
            .page-header {
                display: flex;
                align-items: center;
                justify-content: space-between;
                padding: 15px 20px;
                background-color: #16213e;
                border-bottom: 2px solid #1f2d4a;
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
                background-color: #1f2d4a;
                color: #b0b8c9;
                border: 1px solid #2d3e5f;
                padding: 8px 14px;
                border-radius: 8px;
                cursor: pointer;
                font-size: 13px;
                font-weight: 600;
                transition: background-color 0.2s ease, border-color 0.2s ease, color 0.2s ease, box-shadow 0.2s ease;
                white-space: nowrap;
            }
            .action-btn:hover {
                background-color: #35bf88;
                border-color: #35bf88;
                color: #0a0e27;
                box-shadow: 0 3px 10px rgba(53, 191, 136, 0.4);
            }
            .action-btn:disabled {
                opacity: 0.5;
                cursor: not-allowed;
                background-color: #1f2d4a;
                color: #7a8694;
                border-color: #2d3e5f;
                box-shadow: none;
            }
            .action-btn .btn-icon {
                font-size: 16px;
            }

            /* Scrollbar personalizado */
            ::-webkit-scrollbar {
                width: 10px;
            }
            ::-webkit-scrollbar-track {
                background: #1f2d4a;
            }
            ::-webkit-scrollbar-thumb {
                background: #35bf88;
                border-radius: 5px;
            }
            ::-webkit-scrollbar-thumb:hover {
                background: #2da672;
            }

            /* Embeds en sidebar */
            .embeds-section {
                margin-top: 18px;
                padding-top: 14px;
                border-top: 2px solid #1f2d4a;
            }
            .embeds-section h4 {
                font-size: 12px;
                color: #35bf88;
                margin-bottom: 10px;
                text-transform: uppercase;
                letter-spacing: 0.5px;
            }
            .embed-block {
                margin-bottom: 12px;
            }
            .embed-label {
                font-size: 11px;
                color: #7a8694;
                margin-bottom: 5px;
            }
            .embed-block iframe {
                width: 100% !important;
                border-radius: 6px;
                display: block;
            }

            /* Loader */
            .loader {
                border: 3px solid #1f2d4a;
                border-top: 3px solid #35bf88;
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
                background: #35bf88;
                color: #0a0e27;
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
                background: #e74c3c;
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
                <div class="albums-grid">
    """

    # Procesar cada album
    for album in json_data:
        for group in album["groups"]:
            cover = group.get("cover", "")
            artist = album["artist"]
            album_name = album["album"]
            flac_count = group["flacCount"]
            group_id = group["groupId"]

            # Encontrar la fecha más antigua (original)
            oldest_year = min(
                (t.get("remasterYear", 9999) for t in group["torrents"] if t.get("remasterYear")),
                default="Unknown"
            )

            # Escapar comillas para JavaScript
            artist_escaped = artist.replace("'", "\\'").replace('"', '\\"')
            album_escaped = album_name.replace("'", "\\'").replace('"', '\\"')

            # Crear la portada del álbum con botón eliminar
            html += f"""
                <div class="album" onclick="showTorrents({group_id}, '{artist_escaped}', '{album_escaped}', {oldest_year}, {flac_count})" id="album-{group_id}">
                    <button class="delete-btn" onclick="deleteAlbum({group_id}, event)" title="Eliminar álbum">×</button>
                    <img src="{cover}" alt="{artist} - {album_name}">
                    <div class="album-artist">{artist}</div>
                    <div class="album-name">{album_name}</div>
                    <div class="album-date">({oldest_year}) · {flac_count} FLAC{'s' if flac_count > 1 else ''}</div>
                </div>
            """

            # Guardar los datos de los torrents y embeds en scripts de datos
            youtube_embed = album.get("youtube_embed", "")
            bandcamp_embed = album.get("bandcamp_embed", "")

            html += f"""
                <script>
                    if (!window.torrentData) window.torrentData = {{}};
                    window.torrentData[{group_id}] = {json.dumps(group["torrents"])};
                    if (!window.embedData) window.embedData = {{}};
                    window.embedData[{group_id}] = {{
                        youtube: {json.dumps(youtube_embed)},
                        bandcamp: {json.dumps(bandcamp_embed)}
                    }};
                </script>
            """

    html += """
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

            function showNotification(message, isError = false) {
                const notification = document.getElementById('notification');
                notification.textContent = message;
                notification.className = 'notification show' + (isError ? ' error' : '');

                setTimeout(() => {
                    notification.classList.remove('show');
                }, 3000);
            }

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

                sidebar.innerHTML = tableHtml;
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
