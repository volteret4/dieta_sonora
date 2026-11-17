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
