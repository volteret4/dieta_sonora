FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Sin servidor: este contenedor solo existe para que Ofelia le haga
# "docker exec" a diario (ofelia.job-exec.*), como el resto de jobs de
# tumtumpa. lastfm_stats.db vive en /data (volumen compartido, ver
# docker-compose.yml) para que otros servicios puedan montarla en solo
# lectura y evitar repetir las mismas consultas a la API de Last.fm.
CMD ["sleep", "infinity"]
