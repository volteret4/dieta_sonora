#!/bin/bash
# Orquestador diario del dashboard de estadísticas (release → compra → escucha).
#
# Orden:
#   1. airsonic_checker.py    — crea VTODOs para compras vistas en Airsonic sin tarea
#   2. qbittorrent_checker.py — igual pero desde qBittorrent
#   3. cal_to_estadisticas.py — sincroniza VTODOs -> music_stats.db (fuente de verdad,
#                                incluye matching de escuchas contra lastfm_stats.db si existe)
#   4. extraer_estadisticas.py --export-only — exporta data.json desde music_stats.db
#      (sin volver a tocar CalDAV: cal_to_estadisticas.py ya la dejó al día)
#
# Disparado a diario por Ofelia (ver ofelia.job-exec.dieta-sonora-stats-daily
# en docker-compose.yml), o manualmente con: bash main.sh [--dry-run]
set -e
cd "$(dirname "$0")"

VENV_PYTHON="$HOME/Scripts/python_venv/bin/python3"
if [ -x "$VENV_PYTHON" ]; then
    PYTHON="$VENV_PYTHON"
else
    PYTHON="python3"
fi

SINCE="${STATS_SINCE_DAYS:-14}"
DRY_RUN=()
if [ "$1" = "--dry-run" ]; then
    DRY_RUN=(--dry-run)
    echo "⚠ Modo --dry-run: no se escribirá nada en Radicale/DB."
fi

echo "── 1/4 Airsonic checker (--since $SINCE) ──"
"$PYTHON" airsonic_checker.py --since "$SINCE" "${DRY_RUN[@]}"

echo "── 2/4 qBittorrent checker (--since $SINCE) ──"
"$PYTHON" qbittorrent_checker.py --since "$SINCE" "${DRY_RUN[@]}"

echo "── 3/4 Sync calendario → music_stats.db (--since $SINCE) ──"
# --auto: sin esto, un álbum que MusicBrainz no encuentra hace que el script
# pida la fecha de lanzamiento por input() -- sin stdin (cron/Ofelia) eso es
# un EOFError inmediato que aborta todo el pipeline (set -e).
"$PYTHON" cal_to_estadisticas.py --since "$SINCE" --auto "${DRY_RUN[@]}"

echo "── 4/4 Exportando data.json ──"
"$PYTHON" extraer_estadisticas.py --export-only

echo "✅ Estadísticas actualizadas."
