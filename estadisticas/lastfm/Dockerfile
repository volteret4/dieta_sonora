FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Ofelia le hace "docker exec" a diario (ofelia.job-exec.*), como el
# resto de jobs de tumtumpa -- eso no cambia. server.py añade un único
# endpoint (/api/jobs/run) para poder relanzar el mismo comando a mano
# desde "actualizar todos" (index), por red interna, sin socket de
# Docker -- sin panel ni página, nadie visita este servicio directamente.
# lastfm_stats.db vive en /data (volumen compartido, ver
# docker-compose.yml) para que otros servicios puedan montarla en solo
# lectura y evitar repetir las mismas consultas a la API de Last.fm.
EXPOSE 5000
CMD ["python3", "server.py"]
