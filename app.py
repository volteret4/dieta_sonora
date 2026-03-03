from flask import Flask, render_template, jsonify, request, send_file
import json
import os
import subprocess
import logging
from datetime import datetime
from html_generator import generar_html, enrich_with_embeds
import csv

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# Configuración de rutas
DATA_JSON = "/home/pepe/Scripts/Musica/orpheus-api/resultado_flacs.json"
HTML_OUTPUT = "resumen_flacs.html"
DOWNLOAD_FOLDER = "/mnt/NFS/lidarr/torrents_backup/watch_torrents"
CSV_FILE = "albums.csv"
EMBED_CACHE = "/home/pepe/Scripts/Musica/orpheus-api/embeds_cache.json"

# Rutas de los scripts de las acciones del header
SCRIPT_CALENDARIO = "/home/pepe/Scripts/Musica/orpheus-api/main.sh"   # ← ajusta la ruta
SCRIPT_ESCUCHADOS = "/home/pepe/Scripts/Musica/discos-nuevos/discos_escuchados_calendario.py"
AIRSONIC_URL = "http://192.168.1.133:4040/rest/startScan?u=admin&p=j2WQMyQLX9n9ohkY2vXk&v=1.15.0&c=curl&f=json&fullScan=false"

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def regenerar_html():
    """Regenera el HTML desde el JSON, enriqueciendo con embeds (con caché)"""
    try:
        with open(DATA_JSON, "r", encoding="utf-8") as f:
            json_data = json.load(f)

        # Buscar embeds de YouTube/Bandcamp — usa caché, solo hace peticiones
        # para álbumes nuevos que aún no estén en embeds_cache.json
        json_data = enrich_with_embeds(json_data, cache_file=EMBED_CACHE)

        html = generar_html(json_data)

        with open(HTML_OUTPUT, "w", encoding="utf-8") as f:
            f.write(html)

        logger.info("HTML regenerado correctamente")
        return True
    except Exception as e:
        logger.error(f"Error regenerando HTML: {e}")
        return False


def eliminar_grupo_de_datos(group_id):
    """Lógica unificada para borrar un grupo del JSON, actualizar CSV y HTML"""
    group_id = str(group_id).strip()

    with open(DATA_JSON, "r", encoding="utf-8") as f:
        json_data = json.load(f)

    found = False
    new_json_data = []

    for album in json_data:
        original_len = len(album["groups"])
        # Filtrar el grupo específico
        album["groups"] = [g for g in album["groups"] if str(g.get("groupId")).strip() != group_id]

        if len(album["groups"]) < original_len:
            found = True

        # Solo conservar álbumes que aún tengan grupos
        if len(album["groups"]) > 0:
            new_json_data.append(album)

    if found:
        # Guardar JSON actualizado
        with open(DATA_JSON, "w", encoding="utf-8") as f:
            json.dump(new_json_data, f, ensure_ascii=False, indent=2)

        # Sincronizar otros archivos
        regenerate_csv_from_json(new_json_data)
        regenerar_html()
        return True
    return False

def regenerate_csv_from_json(json_data):
    rows = []

    for album in json_data:
        if album["groups"]:  # solo álbumes que todavía tienen torrents
            rows.append({
                "artist": album["artist"],
                "album": album["album"]
            })

    with open(CSV_FILE, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["artist", "album"])
        writer.writeheader()
        writer.writerows(rows)

@app.route('/')
def index():
    """Página principal"""
    return send_file('index.html')


@app.route('/discos_nuevos')
@app.route('/discos_nuevos.html')
def discos_nuevos():
    """Servir la página de discos nuevos"""
    if os.path.exists(HTML_OUTPUT):
        return send_file(HTML_OUTPUT)
    else:
        return "HTML no encontrado. Por favor, genera el HTML primero.", 404


@app.route('/api/download', methods=['POST'])
def download_torrent():
    """
    Descarga un torrent usando wget, lo mueve a la carpeta configurada
    y utiliza la lógica unificada para limpiar el JSON, CSV y HTML.
    """
    try:
        data = request.json
        download_url = data.get('downloadUrl')
        group_id = data.get('groupId')

        if not download_url or not group_id:
            return jsonify({"error": "Faltan parámetros"}), 400

        logger.info(f"Iniciando descarga de torrent para el grupo: {group_id}")

        # 1. Preparar carpeta de descargas
        os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)
        file_path = os.path.join(DOWNLOAD_FOLDER, f"{group_id}.torrent")

        # 2. Ejecutar descarga con wget
        try:
            result = subprocess.run(
                ['wget', '-O', file_path, download_url],
                capture_output=True,
                text=True,
                timeout=300
            )

            if result.returncode != 0:
                logger.error(f"Error en wget: {result.stderr}")
                return jsonify({"error": f"Error descargando: {result.stderr}"}), 500

            logger.info(f"Descarga completada: {file_path}")

        except subprocess.TimeoutExpired:
            logger.error("La descarga superó el tiempo límite de 5 minutos")
            return jsonify({"error": "Timeout en la descarga"}), 500
        except Exception as e:
            logger.error(f"Error ejecutando wget: {e}")
            return jsonify({"error": str(e)}), 500

        # 3. Lógica de limpieza (Sincroniza JSON, CSV y HTML)
        # Llamamos a la función que ya gestiona todo el borrado
        if eliminar_grupo_de_datos(group_id):
            logger.info(f"Datos actualizados correctamente tras descarga del grupo {group_id}")
            return jsonify({
                "success": True,
                "message": "Torrent descargado y sistema actualizado exitosamente"
            })
        else:
            # Si el torrent se descarga pero no estaba en el JSON por algún motivo
            logger.warning(f"Torrent descargado pero el groupId {group_id} no se encontró en el JSON para eliminar")
            return jsonify({
                "success": True,
                "message": "Torrent descargado, pero no se encontró el registro para eliminar"
            })

    except Exception as e:
        logger.error(f"Error crítico en download_torrent: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/delete', methods=['POST'])
def delete_album():
    data = request.json
    group_id = data.get('groupId')

    if eliminar_grupo_de_datos(group_id):
        return jsonify({"success": True, "message": "Álbum eliminado correctamente"})
    else:
        return jsonify({"error": "Álbum no encontrado"}), 404


@app.route('/api/airsonic', methods=['POST'])
def actualizar_airsonic():
    """Lanza un escaneo en Airsonic"""
    try:
        result = subprocess.run(
            ['curl', '-s', AIRSONIC_URL],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            return jsonify({"error": result.stderr}), 500
        logger.info("Escaneo Airsonic lanzado correctamente")
        return jsonify({"success": True, "message": "Escaneo iniciado en Airsonic", "response": result.stdout})
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timeout al contactar con Airsonic"}), 500
    except Exception as e:
        logger.error(f"Error en actualizar_airsonic: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/calendario', methods=['POST'])
def revisar_calendario():
    """Ejecuta el script bash de revisión de calendario"""
    try:
        result = subprocess.run(
            ['bash', SCRIPT_CALENDARIO],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            return jsonify({"error": result.stderr or "El script terminó con error"}), 500
        logger.info("Script de calendario ejecutado correctamente")
        return jsonify({"success": True, "message": "Calendario revisado correctamente", "output": result.stdout})
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timeout ejecutando el script de calendario"}), 500
    except FileNotFoundError:
        return jsonify({"error": f"Script no encontrado: {SCRIPT_CALENDARIO}"}), 500
    except Exception as e:
        logger.error(f"Error en revisar_calendario: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/escuchados', methods=['POST'])
def discos_escuchados():
    """Ejecuta el script Python de discos escuchados"""
    try:
        result = subprocess.run(
            ['python3', SCRIPT_ESCUCHADOS],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode != 0:
            return jsonify({"error": result.stderr or "El script terminó con error"}), 500
        logger.info("Script de discos escuchados ejecutado correctamente")
        return jsonify({"success": True, "message": "Discos escuchados procesados", "output": result.stdout})
    except subprocess.TimeoutExpired:
        return jsonify({"error": "Timeout ejecutando el script de discos escuchados"}), 500
    except FileNotFoundError:
        return jsonify({"error": f"Script no encontrado: {SCRIPT_ESCUCHADOS}"}), 500
    except Exception as e:
        logger.error(f"Error en discos_escuchados: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/refresh_embeds', methods=['POST'])
def refresh_embeds():
    """
    Fuerza la re-búsqueda de embeds para un álbum concreto eliminando
    su entrada de la caché. Luego regenera el HTML completo.
    Parámetros JSON: { "artist": "...", "album": "..." }
    Si no se pasan, limpia TODA la caché y re-busca todos los álbumes.
    """
    try:
        data = request.json or {}
        artist = data.get("artist")
        album_name = data.get("album")

        # Manipular caché
        cache = {}
        if os.path.exists(EMBED_CACHE):
            with open(EMBED_CACHE, "r", encoding="utf-8") as f:
                cache = json.load(f)

        if artist and album_name:
            cache_key = f"{artist}|||{album_name}"
            if cache_key in cache:
                del cache[cache_key]
                with open(EMBED_CACHE, "w", encoding="utf-8") as f:
                    json.dump(cache, f, ensure_ascii=False, indent=2)
                msg = f"Caché eliminada para '{artist} – {album_name}'"
            else:
                msg = f"'{artist} – {album_name}' no estaba en caché"
        else:
            # Limpiar toda la caché
            with open(EMBED_CACHE, "w", encoding="utf-8") as f:
                json.dump({}, f)
            msg = "Caché de embeds limpiada completamente"

        logger.info(msg)

        # Regenerar HTML (hará las nuevas búsquedas)
        if regenerar_html():
            return jsonify({"success": True, "message": msg + ". HTML regenerado."})
        else:
            return jsonify({"error": "Error regenerando HTML"}), 500

    except Exception as e:
        logger.error(f"Error en refresh_embeds: {e}")
        return jsonify({"error": str(e)}), 500


@app.route('/api/status')
def status():
    """Endpoint para verificar el estado del servidor"""
    cache_count = 0
    if os.path.exists(EMBED_CACHE):
        with open(EMBED_CACHE, "r", encoding="utf-8") as f:
            cache_count = len(json.load(f))
    return jsonify({
        "status": "running",
        "data_file": os.path.exists(DATA_JSON),
        "html_file": os.path.exists(HTML_OUTPUT),
        "download_folder": DOWNLOAD_FOLDER,
        "embed_cache_entries": cache_count,
        "embed_cache_file": EMBED_CACHE,
    })


if __name__ == '__main__':
    # Verificar que existen los archivos necesarios
    if not os.path.exists(DATA_JSON):
        logger.warning(f"Archivo {DATA_JSON} no encontrado")

    # Generar HTML inicial si no existe
    if not os.path.exists(HTML_OUTPUT):
        logger.info("Generando HTML inicial...")
        regenerar_html()

    # Iniciar servidor
    app.run(host='0.0.0.0', port=5001, debug=True)
