# dieta_sonora

Parte de [REPEAT-ONE](https://github.com/volteret4/repeat-one-web). Dos herramientas sobre los
discos nuevos de los artistas que sigues:

- **Lanzamientos**: una web con los discos pendientes de conseguir (desde tu calendario CalDAV),
  con reproductores de YouTube y Bandcamp, descarga por qBittorrent y aviso a Airsonic/Navidrome.
- **Estadísticas**: cuánto tarda cada disco desde que sale hasta que lo consigues y lo escuchas.

Hay una [versión pública para el navegador](https://volteret4.github.io/repeat-one-web/dieta-sonora/)
que trae los discos de tu cuenta de Muspy.

## Cómo funciona

Los lanzamientos viven en un calendario de Radicale: un evento de día completo por disco
(`Artista - Disco`) y una tarea por disco con `DTSTART` = cuándo lo conseguiste y
`COMPLETED` = cuándo lo escuchaste. Los scripts del repo
[Scripts](https://github.com/volteret4/Scripts/tree/main/Musica/discos-nuevos) llenan ese
calendario desde Muspy y marcan lo escuchado desde Last.fm.

## Instalación

Con el instalador de REPEAT-ONE (módulo `dieta_sonora`), o a mano:

```bash
cp .env.example .env
```

Rellena `.env` (o usa el panel ⚙ de la web) y levanta el servicio con el `docker-compose.yml`
de REPEAT-ONE. Las rutas de tu NAS se montan como volúmenes en `/music/...`; ningún valor de
este repo apunta a una instalación concreta.

Tarea diaria: `docker exec dieta-sonora bash main.sh`.

## Instalación con Docker Compose

```bash
cp .env.example .env
```

Rellena `.env` (credenciales y carpetas de tu equipo) y arranca:

```bash
docker compose up -d --build
```

Los datos que genera (bases, cachés) se quedan en esta misma carpeta, fuera de git.

## Actualizaciones automáticas

El `docker-compose.yml` trae su propia [Ofelia](https://github.com/mcuadros/ofelia), que lanza dentro del contenedor los mismos trabajos y a las mismas horas que en REPEAT-ONE (formato con segundos: `seg min hora día mes día-semana`, hora de `TZ`, por defecto Europe/Madrid):

| Trabajo | Cuándo | Qué lanza |
|---|---|---|
| `dieta-sonora-daily` | `0 0 1 * * *` | `bash main.sh` |
| `dieta-sonora-stats-daily` | `0 15 1 * * *` | `bash main.sh` |
| `dieta-sonora-stats-genres` | `0 15 4 * * 0` | `python3 extraer_estadisticas.py` |
| `dieta-sonora-stats-scrobble-years` | `0 45 1 * * *` | `python3 enrich_scrobble_years.py --limit 2000` |

Para cambiar una hora, edita su etiqueta `ofelia.job-exec.<trabajo>.schedule` en `docker-compose.yml` y vuelve a hacer `docker compose up -d`. Para lanzarlo a mano: `docker compose exec <servicio> <orden>`. Para no tener actualizaciones automáticas: `docker compose stop ofelia`.

Esta Ofelia solo ve los contenedores de este repo (etiqueta `repeat-one.ofelia`), así que puedes tener varios módulos instalados por separado sin que se pisen. Necesita el socket de Docker en solo lectura.
