#!/usr/bin/env python3
"""
Servidor estático para el dashboard de estadísticas (index.html / estadisticas.html).
Ambos fetchean data.json vía JS, por lo que necesitan servirse desde un
servidor (no abrirse como file://). Genera data.json/stats.json con
extraer_estadisticas.py o cal_to_estadisticas.py antes de usarlo.
"""
import os
from pathlib import Path
from flask import Flask, abort, jsonify, request, send_from_directory

BASE_DIR = Path(__file__).parent
app = Flask(__name__)


@app.route("/", defaults={"path": "index.html"})
@app.route("/<path:path>")
def serve_static(path):
    if path.startswith("api/") or path in ("theme-picker.js", "theme-palettes.css", "settings-panel.js"):
        abort(404)  # dejar paso a las rutas dedicadas de abajo
    file_path = BASE_DIR / path
    if not file_path.exists() or not file_path.is_file():
        abort(404)
    return send_from_directory(str(BASE_DIR), path)


@app.route("/theme-palettes.css")
def theme_palettes_css():
    return send_from_directory(str(BASE_DIR), "theme-palettes.css")


@app.route("/theme-picker.js")
def theme_picker_js():
    return send_from_directory(str(BASE_DIR), "theme-picker.js")


@app.route("/settings-panel.js")
def settings_panel_js():
    return send_from_directory(str(BASE_DIR), "settings-panel.js")


# ── Panel de configuración (⚙) ───────────────────────────────────────────────
# Mismo patrón que dieta_sonora/app.py. Todas las vars aquí son "service"
# (sin bind mounts en este contenedor): se leen/escriben en el .env propio
# (services/dieta_sonora/estadisticas/albums_log/.env, montado en /app/.env).
# Las usan cal_to_estadisticas.py / airsonic_checker.py / qbittorrent_checker.py
# / extraer_estadisticas.py cuando se lanzan (p.ej. vía docker exec) — no
# server.py, que solo sirve los HTML estáticos.
SETTINGS_ENV_PATH = BASE_DIR / ".env"
SETTINGS_PASSWORD = os.getenv("SETTINGS_PASSWORD", "")
VARS_SPEC = [
    {"name": "RADICALE_URL", "secret": False, "help": "URL base del servidor Radicale"},
    {"name": "RADICALE_USERNAME", "secret": False, "help": "Usuario Radicale"},
    {"name": "RADICALE_PW", "secret": True, "help": "Contraseña Radicale"},
    {"name": "RADICALE_CALENDAR", "secret": False, "help": "Ruta del calendario de lanzamientos"},
    {"name": "CALENDAR_NAME", "secret": False, "help": "Nombre del calendario de lanzamientos"},
    {"name": "CALENDAR_TASKS", "secret": False, "help": "Ruta del calendario de tareas"},
    {"name": "MB_EMAIL", "secret": False, "help": "Email de contacto para la API de MusicBrainz"},
    {"name": "LASTFM_API_KEY", "secret": True, "help": "API key de Last.fm (matching de scrobbles)"},
    {"name": "LASTFM_USERNAME", "secret": False, "help": "Usuario de Last.fm (extracción de scrobbles)"},
    {"name": "LB_USERNAME", "secret": False, "help": "Usuario de ListenBrainz"},
    {"name": "LB_TOKEN", "secret": True, "help": "Token de ListenBrainz"},
    {"name": "AIRSONIC_URL", "secret": False, "help": "URL base de Airsonic"},
    {"name": "AIRSONIC_USER", "secret": False, "help": "Usuario Airsonic"},
    {"name": "AIRSONIC_PASS", "secret": True, "help": "Contraseña Airsonic"},
    {"name": "AIRSONIC_API_VERSION", "secret": False, "help": "Versión de API de Airsonic (ej. 1.15.0)"},
    {"name": "QB_HOST", "secret": False, "help": "Host de qBittorrent"},
    {"name": "QB_PORT", "secret": False, "help": "Puerto de qBittorrent"},
    {"name": "QB_USER", "secret": False, "help": "Usuario qBittorrent"},
    {"name": "QB_PASS", "secret": True, "help": "Contraseña qBittorrent"},
]
_HAS_SECRETS = any(v.get("secret") for v in VARS_SPEC)


def _read_env_file(path):
    values = {}
    if not os.path.exists(path):
        return values
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, v = s.split("=", 1)
            v = v.strip()
            if len(v) >= 2 and v[0] == v[-1] and v[0] in ('"', "'"):
                v = v[1:-1]
            values[k.strip()] = v
    return values


def _write_env_file(path, updates):
    lines = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    seen = set()
    out = []
    for line in lines:
        s = line.strip()
        if s and not s.startswith("#") and "=" in s:
            k = s.split("=", 1)[0].strip()
            if k in updates:
                out.append(f"{k}={updates[k]}\n")
                seen.add(k)
                continue
        out.append(line)
    for k, v in updates.items():
        if k not in seen:
            if out and not out[-1].endswith("\n"):
                out[-1] += "\n"
            out.append(f"{k}={v}\n")
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(out)


def _current_value(spec):
    file_vals = _read_env_file(SETTINGS_ENV_PATH)
    if spec["name"] in file_vals:
        return file_vals[spec["name"]]
    return os.environ.get(spec["name"], spec.get("default", ""))


def _check_auth(password):
    if not SETTINGS_PASSWORD:
        return not _HAS_SECRETS
    return password == SETTINGS_PASSWORD


@app.route("/api/settings", methods=["POST"])
def api_settings():
    d = request.get_json(silent=True) or {}
    password = d.get("password") or ""
    requires = bool(SETTINGS_PASSWORD) or _HAS_SECRETS
    authorized = _check_auth(password)
    if requires and not authorized:
        error = "Contraseña incorrecta" if password else None
        if not SETTINGS_PASSWORD:
            error = "Este servicio tiene credenciales pero no hay SETTINGS_PASSWORD configurada. Añádela al .env y reinicia el contenedor."
        return jsonify({"requires_password": True, "authorized": False, "error": error})
    vars_out = [
        {"name": v["name"], "value": _current_value(v), "secret": v["secret"], "help": v.get("help", "")}
        for v in VARS_SPEC
    ]
    return jsonify({"requires_password": requires, "authorized": True, "vars": vars_out})


@app.route("/api/settings/save", methods=["POST"])
def api_settings_save():
    d = request.get_json(silent=True) or {}
    if not _check_auth(d.get("password") or ""):
        return jsonify({"error": "Contraseña incorrecta"}), 403
    known = {v["name"] for v in VARS_SPEC}
    updates = {k: v for k, v in (d.get("values") or {}).items() if k in known}
    if not updates:
        return jsonify({"error": "Nada que guardar"}), 400
    _write_env_file(SETTINGS_ENV_PATH, updates)
    return jsonify({"ok": True, "message": "Guardado. Reinicia el contenedor para aplicar los cambios."})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8768))
    app.run(host="0.0.0.0", port=port)
