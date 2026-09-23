#!/usr/bin/env python3
"""
Mini servidor solo para /api/jobs/run -- este servicio no tiene web propia
(antes CMD ["sleep", "infinity"], Ofelia le hacía docker exec sin más).
Se añade el mínimo imprescindible para que "actualizar todos" (index) y,
si hiciera falta, un trigger manual, puedan relanzar extraer_scrobbles.py
por red interna en vez de por docker exec -- index/app.py no tiene (ni
debe tener) el socket de Docker, así que todo trigger es HTTP.

No hay panel ⚙ ni página -- nadie visita este servicio directamente.
"""
import os
import signal
import subprocess
from pathlib import Path

from flask import Flask, jsonify, request

BASE_DIR = Path(__file__).parent
app = Flask(__name__)

# Mismo comando que ya lanza Ofelia vía docker exec (ofelia.job-exec.
# lastfm-scrobbles-daily en docker-compose.yml) -- duplicado a propósito,
# el contenedor no puede leer esas labels en runtime.
JOBS = [
    {"id": "lastfm-scrobbles-daily", "label": "Extraer scrobbles (Last.fm + ListenBrainz)",
     "cmd": ["python3", "extraer_scrobbles.py"], "timeout": 1800},
]


@app.route("/health")
def health():
    return jsonify({"ok": True})


def _run_job_cmd(cmd, cwd, timeout):
    """subprocess.run(timeout=...) solo mata al hijo directo -- sus propios
    hijos quedan huérfanos y siguen corriendo (confirmado en producción con
    dieta-sonora-stats). start_new_session=True + os.killpg mata el grupo
    entero al hacer timeout."""
    proc = subprocess.Popen(
        cmd, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, start_new_session=True,
    )
    try:
        stdout, _ = proc.communicate(timeout=timeout)
        return proc.returncode, stdout
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait()
        raise


@app.route("/api/jobs/run", methods=["POST"])
def api_jobs_run():
    job_id = (request.get_json(silent=True) or {}).get("job")
    job = next((j for j in JOBS if j["id"] == job_id), None)
    if not job:
        return jsonify({"error": "Job desconocido"}), 404
    try:
        returncode, output = _run_job_cmd(job["cmd"], str(BASE_DIR), job["timeout"])
        if returncode != 0:
            return jsonify({"error": (output or "")[-2000:] or "Error ejecutando el job"}), 500
        return jsonify({"ok": True, "message": f"{job['label']} completado"})
    except subprocess.TimeoutExpired:
        return jsonify({"error": f"Tardó más de {job['timeout']}s (proceso terminado)"}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
