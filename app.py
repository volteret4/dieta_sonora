import os
import re
import shutil
import glob
import json
import csv
import uuid
from datetime import datetime, date, timedelta
import logging
import subprocess
import unicodedata
import threading
from datetime import datetime
from xml.etree import ElementTree as ET
from mutagen.flac import FLAC
from wakeonlan import send_magic_packet

from flask import Flask, render_template, jsonify, request, send_file, send_from_directory
from flask_cors import CORS
import requests
from icalendar import Calendar

# Importaciones locales (asegúrate de que los archivos estén en el mismo dir o PYTHONPATH)
from tools.sops_env import load_sops_env
from html_generator import generar_html, enrich_with_embeds

# --- INICIALIZACIÓN ---
load_sops_env()
app = Flask(__name__)
CORS(app) # Habilita CORS para todas las rutas
app.config['JSON_AS_ASCII'] = False

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURACIÓN MÓDULO MÚSICA (app.py) ---
DATA_JSON = "/home/pepe/gits/pollo/dieta_sonora/resultado_flacs.json"
HTML_OUTPUT = "/home/pepe/gits/pollo/dieta_sonora/resumen_flacs.html"
DOWNLOAD_FOLDER = "/mnt/NFS/lidarr/torrents_backup/watch_torrents"
BASE_PATH = "/mnt/NFS/lidarr/torrents_backup"
MOODE_PATH = "/mnt/NFS/moode/moode/temp/auto-dietpi"
CSV_FILE = "/home/pepe/gits/pollo/dieta_sonora/albums.csv"
EMBED_CACHE = "/home/pepe/gits/pollo/dieta_sonora/embeds_cache.json"
SCRIPT_CALENDARIO = "/home/pepe/gits/pollo/dieta_sonora/main.sh"
SCRIPT_ESCUCHADOS = "/home/pepe/gits/pollo/dieta_sonora/tools/discos_escuchados_calendario.py"
AIRSONIC_URL = "http://192.168.1.133:4040/rest/startScan?u=admin&p=j2WQMyQLX9n9ohkY2vXk&v=1.15.0&c=curl&f=json&fullScan=false"

RADICALE_URL   = os.getenv('RADICALE_URL', '').rstrip('/')
RADICALE_USER  = os.getenv('RADICALE_USERNAME', '')
RADICALE_PW    = os.getenv('RADICALE_PW', '')
RADICALE_BASE  = os.getenv('RADICALE_CALENDAR', '').rstrip('/')
CALENDAR_TASKS = os.getenv('CALENDAR_TASKS', '')

# --- CONFIGURACIÓN MÓDULO PODCAST/TTS (server.py) ---
# Ajusta WORK_DIR a la ruta real de tus scripts de podcast
WORK_DIR = os.getenv('TTS_WORK_DIR', '/home/pepe/contenedores/podcast-tts')
SELECTION_FILE = os.path.join(WORK_DIR, "selection.json")
LOG_FILE = os.path.join(WORK_DIR, "conversion_log.txt")
ARTICLES_DATA_FILE = os.path.join(WORK_DIR, "articles_data.json")
STATUS_FILE = os.path.join(WORK_DIR, "conversion_status.json")

os.makedirs(WORK_DIR, exist_ok=True)

# Radicale
CALENDAR_URL = "https://radicale.pollete.duckdns.org/pollo/982339b6-2686-86aa-068c-d6dcdb8f712c/"
TASKS_URL = "https://radicale.pollete.duckdns.org/pollo/00169e81-e5f4-d26a-d1c9-23a3dad5ea5b/"

# Telegram
TELEGRAM_TOKEN = os.getenv('TELEGRAM_TOKEN')
CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

# Estado global del proceso TTS
conversion_status = {
    "running": False,
    "progress": 0,
    "total": 0,
    "current_article": "",
    "started_at": None,
    "finished_at": None,
    "errors": []
}

# -----------------------------------------------------------------------------
# HELPER FUNCTIONS (MÓDULO MÚSICA)
# -----------------------------------------------------------------------------

def _normalize(s: str) -> str:
    s = re.sub(r'\s+', ' ', s.strip().lower())
    s = unicodedata.normalize('NFD', s)
    return ''.join(c for c in s if unicodedata.category(c) != 'Mn')

def _parse_summary(summary: str) -> tuple[str, str]:
    summary = re.sub(r'^[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+|[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+$', '', summary).strip()
    parts = re.split(r'\s+[-–—]\s+', summary, maxsplit=1)
    return (parts[0].strip(), parts[1].strip()) if len(parts) == 2 else (summary, '')

def find_album_for_group(json_data, group_id):
    group_id = str(group_id).strip()
    for album in json_data:
        for g in album.get('groups', []):
            if str(g.get('groupId', '')).strip() == group_id:
                return album.get('artist'), album.get('album')
    return None, None

def regenerar_html():
    """Regenera el HTML desde DATA_JSON. Lanza excepción si falla."""
    with open(DATA_JSON, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    json_data = enrich_with_embeds(json_data, cache_file=EMBED_CACHE)
    html = generar_html(json_data)
    with open(HTML_OUTPUT, "w", encoding="utf-8") as f:
        f.write(html)

def _read_csv_types() -> dict[tuple, str]:
    """Devuelve {(artist_norm, album_norm): type} del CSV actual."""
    types = {}
    if not os.path.exists(CSV_FILE):
        return types
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        for row in csv.DictReader(f):
            key = (_normalize(row.get('artist', '')), _normalize(row.get('album', '')))
            types[key] = row.get('type', 'vevent')
    return types


def eliminar_grupo_de_datos(group_id):
    group_id = str(group_id).strip()
    with open(DATA_JSON, "r", encoding="utf-8") as f:
        json_data = json.load(f)

    found = False
    new_json_data = []
    for album in json_data:
        original_len = len(album["groups"])
        album["groups"] = [g for g in album["groups"] if str(g.get("groupId")).strip() != group_id]
        if len(album["groups"]) < original_len: found = True
        if len(album["groups"]) > 0: new_json_data.append(album)

    if found:
        with open(DATA_JSON, "w", encoding="utf-8") as f:
            json.dump(new_json_data, f, ensure_ascii=False, indent=2)
        existing_types = _read_csv_types()
        rows = []
        for a in new_json_data:
            if a["groups"]:
                key = (_normalize(a["artist"]), _normalize(a["album"]))
                rows.append({"artist": a["artist"], "album": a["album"],
                              "type": existing_types.get(key, "vevent")})
        with open(CSV_FILE, "w", newline='', encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["artist", "album", "type"])
            writer.writeheader()
            writer.writerows(rows)
        return True
    return False


def delete_vtodo_in_radicale(artist: str, album: str) -> bool:
    """Busca el VTODO de 'artist — album' en Radicale y lo elimina."""
    if not RADICALE_URL or not CALENDAR_TASKS:
        logger.warning('Radicale no configurado, no se puede eliminar VTODO')
        return False

    artist_n = _normalize(artist)
    album_n  = _normalize(album)
    url = f'{RADICALE_URL}{RADICALE_BASE}/{CALENDAR_TASKS}/'
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">'
        '  <D:prop><D:getetag/><C:calendar-data/></D:prop>'
        '  <C:filter><C:comp-filter name="VCALENDAR"/></C:filter>'
        '</C:calendar-query>'
    )
    try:
        r = requests.request('REPORT', url, data=body.encode('utf-8'),
                             headers={'Depth': '1', 'Content-Type': 'application/xml; charset=utf-8'},
                             auth=(RADICALE_USER, RADICALE_PW), timeout=30)
        r.raise_for_status()
    except Exception as e:
        logger.error(f'Error obteniendo VTODOs: {e}')
        return False

    ns = {'D': 'DAV:', 'C': 'urn:ietf:params:xml:ns:caldav'}
    root = ET.fromstring(r.content)
    for resp in root.findall('.//D:response', ns):
        href_el  = resp.find('D:href', ns)
        cal_data = resp.find('.//C:calendar-data', ns)
        if href_el is None or cal_data is None or not cal_data.text:
            continue
        try:
            cal = Calendar.from_ical(cal_data.text)
        except Exception:
            continue
        for comp in cal.walk():
            if getattr(comp, 'name', '') != 'VTODO':
                continue
            a, b = _parse_summary(str(comp.get('SUMMARY', '')))
            if _normalize(a) == artist_n and _normalize(b) == album_n:
                filename = os.path.basename(href_el.text.rstrip('/'))
                del_url = f'{RADICALE_URL}{RADICALE_BASE}/{CALENDAR_TASKS}/{filename}'
                try:
                    dr = requests.delete(del_url, auth=(RADICALE_USER, RADICALE_PW), timeout=15)
                    if dr.status_code in (200, 204):
                        logger.info(f'VTODO eliminado: {artist} — {album}')
                        return True
                    logger.error(f'Error al eliminar VTODO: HTTP {dr.status_code}')
                except Exception as e:
                    logger.error(f'Error al eliminar VTODO: {e}')
                return False

    logger.warning(f'VTODO no encontrado: {artist} — {album}')
    return False

# -----------------------------------------------------------------------------
# HELPER FUNCTIONS (MÓDULO TTS)
# -----------------------------------------------------------------------------

def update_tts_status(progress=None, total=None, current_article=None, error=None, finished=False):
    global conversion_status
    if progress is not None: conversion_status["progress"] = progress
    if total is not None: conversion_status["total"] = total
    if current_article is not None: conversion_status["current_article"] = current_article
    if error is not None: conversion_status["errors"].append(error)
    if finished:
        conversion_status["running"] = False
        conversion_status["finished_at"] = datetime.now().isoformat()

    with open(STATUS_FILE, 'w') as f:
        json.dump(conversion_status, f, indent=2)

def run_tts_conversion():
    global conversion_status
    try:
        update_tts_status(progress=0, current_article="Iniciando conversión...")
        os.chdir(WORK_DIR)

        cmd = ['python3', '/home/pepe/contenedores/podcast-tts/process_selection.py', '--selection', SELECTION_FILE, '--generate-feed']
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)

        with open(LOG_FILE, 'w') as log:
            for line in process.stdout:
                log.write(line)
                log.flush()
                if "Procesando" in line:
                    # Aquí podrías añadir el regex de parseo del server.py original
                    update_tts_status(current_article=line.strip())

        process.wait()
        update_tts_status(finished=True, current_article="¡Conversión completada!")
    except Exception as e:
        update_tts_status(error=str(e), finished=True)

# -----------------------------------------------------------------------------
# RUTAS DE LA API (MÚSICA)
# -----------------------------------------------------------------------------

@app.route('/')
def index():
    return send_file('index.html')

@app.route('/discos_nuevos')
def discos_nuevos():
    return send_file(HTML_OUTPUT) if os.path.exists(HTML_OUTPUT) else ("No encontrado", 404)

@app.route('/api/albums')
def api_albums():
    """Sirve el JSON de álbumes enriquecido con embeds (fuente de verdad para el frontend)."""
    with open(DATA_JSON, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    json_data = enrich_with_embeds(json_data, cache_file=EMBED_CACHE)
    return jsonify(json_data)


@app.route('/api/delete', methods=['POST'])
def delete_album():
    data = request.json
    group_id = data.get('groupId')
    with open(DATA_JSON, 'r', encoding='utf-8') as f:
        json_data = json.load(f)
    artist, album = find_album_for_group(json_data, group_id)

    if not eliminar_grupo_de_datos(group_id):
        return jsonify({"error": "Álbum no encontrado"}), 404

    msg = "Álbum eliminado correctamente"
    if artist and album:
        if delete_vtodo_in_radicale(artist, album):
            msg += " (VTODO eliminado de Radicale)"
        else:
            msg += " (VTODO no encontrado en Radicale)"
    return jsonify({"success": True, "message": msg})


@app.route('/api/download', methods=['POST'])
def download_torrent():
    data = request.json
    download_url = data.get('downloadUrl')
    group_id = data.get('groupId')

    # Lógica simplificada de descarga
    file_path = os.path.join(DOWNLOAD_FOLDER, f"{group_id}.torrent")
    res = subprocess.run(['wget', '-O', file_path, download_url])

    if res.returncode == 0 and eliminar_grupo_de_datos(group_id):
        return jsonify({"success": True, "message": "Descargado y actualizado"})
    return jsonify({"error": "Error en proceso"}), 500

# -----------------------------------------------------------------------------
# RUTAS DE LA API (TTS / PODCAST)
# -----------------------------------------------------------------------------

@app.route('/api/articles_data.json')
def articles_data():
    return send_from_directory(WORK_DIR, 'articles_data.json')

@app.route('/api/save-selection', methods=['POST'])
def save_selection():
    global conversion_status
    if conversion_status["running"]:
        return jsonify({"success": False, "message": "Conversión en curso"}), 400

    selection = request.json
    with open(SELECTION_FILE, 'w', encoding='utf-8') as f:
        json.dump(selection, f, ensure_ascii=False, indent=2)

    conversion_status = {
        "running": True, "progress": 0, "total": 10, # Simplificado
        "current_article": "Iniciando...", "started_at": datetime.now().isoformat(),
        "finished_at": None, "errors": []
    }

    thread = threading.Thread(target=run_tts_conversion)
    thread.start()
    return jsonify({"success": True, "message": "Conversión iniciada"})

@app.route('/api/conversion-status')
def get_tts_status():
    return jsonify(conversion_status)

# -----------------------------------------------------------------------------
# STATUS GLOBAL Y EJECUCIÓN
# -----------------------------------------------------------------------------

@app.route('/api/global-status')
def global_status():
    return jsonify({
        "music": {"html": os.path.exists(HTML_OUTPUT), "json": os.path.exists(DATA_JSON)},
        "tts": {"running": conversion_status["running"], "work_dir": WORK_DIR}
    })

# -----------------------------------------------------------------------------
# API_POLLO
# -----------------------------------------------------------------------------

# wakeonlan
@app.route('/wakeonlan', methods=['POST'])
def wol_endpoint():
    mac = request.form.get('mac')
    if mac:
        send_magic_packet(mac)
        return f"Magic packet enviado a {mac}", 200
    return "Falta MAC", 400


# Mover disco descargado.
@app.route('/copiar-album', methods=['POST'])
def copiar_album():
    data = request.json
    album_folder = data.get('album')

    # 1. Construir ruta completa al álbum
    ruta_origen = os.path.join(BASE_PATH, album_folder)
    if not os.path.exists(ruta_origen):
        return jsonify({"error": "Origen no existe"}), 404

    try:
        # 1. Obtener Metadatos FLAC
        archivos_en_carpeta = os.listdir(ruta_origen)
        # Filtramos los archivos que terminen en .flac (sin importar mayúsculas/minúsculas)
        flac_files = [f for f in archivos_en_carpeta if f.lower().endswith('.flac')]

        if not flac_files:
            return jsonify({"error": f"No hay archivos FLAC en {ruta_origen}"}), 400

        # Construimos la ruta completa para el primer archivo encontrado
        primer_flac = os.path.join(ruta_origen, flac_files[0])
        audio = FLAC(primer_flac)
        artist = audio.get('albumartist', audio.get('artist', ['Unknown Artist']))[0]
        album_name = audio.get('album', ['Unknown Album'])[0]
        lanzamiento = f"{artist} - {album_name}"

        # 2. Gestionar Carpetas y Copiado
        fecha_hoy = datetime.now().strftime('%Y-%m-%d')
        destino_dia = os.path.join(MOODE_PATH, fecha_hoy)
        os.makedirs(destino_dia, exist_ok=True)

        ruta_final = os.path.join(destino_dia, lanzamiento)
        shutil.copytree(ruta_origen, ruta_final, dirs_exist_ok=True)

        # 3. Preparar Fechas para iCalendar
        ahora_ics = datetime.now().strftime("%Y%m%dT%H%M%SZ")
        due_ics = (datetime.now() + timedelta(days=90)).strftime("%Y%m%dT%H%M%SZ")
        event_uid = str(uuid.uuid4())

        # 4. Crear Evento en Radicale
        ics_event = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Python Flask//ES
BEGIN:VEVENT
UID:{event_uid}
DTSTAMP:{ahora_ics}
DTSTART:{ahora_ics}
SUMMARY:{lanzamiento}
END:VEVENT
END:VCALENDAR"""

        requests.put(f"{CALENDAR_URL}{event_uid}.ics",
                        data=ics_event.encode('utf-8'),
                        auth=(RADICALE_USER, RADICALE_PW),
                        headers={"Content-Type": "text/calendar; charset=utf-8"})

        # 5. Crear Tarea en Radicale
        ics_task = f"""BEGIN:VCALENDAR
VERSION:2.0
PRODID:-//Python Flask//ES
BEGIN:VTODO
UID:{event_uid}
DTSTAMP:{ahora_ics}
DTSTART:{ahora_ics}
DUE:{due_ics}
SUMMARY:{lanzamiento}
END:VTODO
END:VCALENDAR"""

        requests.put(f"{TASKS_URL}{event_uid}.ics",
                        data=ics_task.encode('utf-8'),
                        auth=(RADICALE_USER, RADICALE_PW),
                        headers={"Content-Type": "text/calendar; charset=utf-8"})

        # 6. Notificación Telegram
        msg = f"Se ha descargado '{lanzamiento}'"
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                        json={"chat_id": CHAT_ID, "text": msg})

        return jsonify({"status": "success", "lanzamiento": lanzamiento}), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    # Generar HTML inicial si falta
    if not os.path.exists(HTML_OUTPUT):
        regenerar_html()

    logger.info("Iniciando Servidor Unificado en puerto 5001")
    app.run(host='0.0.0.0', port=5001, debug=True, threaded=True)
