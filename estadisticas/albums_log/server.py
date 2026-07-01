#!/usr/bin/env python3
"""
Servidor estático para el dashboard de estadísticas (index.html / estadisticas.html).
Ambos fetchean data.json vía JS, por lo que necesitan servirse desde un
servidor (no abrirse como file://). Genera data.json/stats.json con
extraer_estadisticas.py o cal_to_estadisticas.py antes de usarlo.
"""
import os
from pathlib import Path
from flask import Flask, send_from_directory, abort

BASE_DIR = Path(__file__).parent
app = Flask(__name__)


@app.route("/", defaults={"path": "index.html"})
@app.route("/<path:path>")
def serve_static(path):
    file_path = BASE_DIR / path
    if not file_path.exists() or not file_path.is_file():
        abort(404)
    return send_from_directory(str(BASE_DIR), path)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8768))
    app.run(host="0.0.0.0", port=port)
