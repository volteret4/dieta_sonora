# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**dieta_sonora** is a personal music management system with two main components:
1. **CSV Lanzamientos** — tracks pending album purchases via a Flask web dashboard
2. **Web Estadísticas** — tracks time from release → purchase → listen using Radicale CalDAV as source of truth

## Architecture

### Data Flow
```
Radicale CalDAV
├─ VEVENT (calendar events) → album release dates
└─ VTODO (tasks) → purchase (DTSTART) + listen (COMPLETED) dates
         ↓
revisor_calendario.py → albums.csv (pending purchases)
         ↓
airsonic_checker.py / qbittorrent_checker.py → clean albums.csv
         ↓
estadisticas/albums_log/cal_to_estadisticas.py → SQLite + JSON for stats web
         ↓
html_generator.py → resumen_flacs.html (dashboard with YouTube/Bandcamp embeds)
         ↓
app.py (Flask) → serves dashboard, handles torrent downloads, triggers Airsonic scans
```

### Key Scripts
- **`app.py`** — Flask server (port from env), main dashboard at `/discos_nuevos`
- **`html_generator.py`** — generates dark-themed HTML with YouTube/Bandcamp embeds; caches to `embeds_cache.json`
- **`revisor_calendario.py`** — exports Radicale VEVENT to `albums.csv`
- **`main.sh`** — daily orchestration: revisor → airsonic clean → regenerate HTML
- **`estadisticas/albums_log/cal_to_estadisticas.py`** — syncs CalDAV to SQLite + JSON for stats
- **`estadisticas/albums_log/extraer_estadisticas.py`** — enriches data with MusicBrainz/Last.fm genres

### Secret Management
Secrets use **SOPS + age** (see `.sops.yaml`). Previously used `python-dotenv` with `.env` files.
- Encrypted secrets: `.encrypted.env`
- Age public key in `.sops.yaml`
- Pre-commit hook: gitleaks (detects accidental secret commits)

To decrypt secrets for use in scripts:
```bash
sops -d .encrypted.env
```

## Tasks (Pending)

### 1. SOPS Bridge Script
Create a bridge script to load SOPS-decrypted secrets into existing scripts that use `python-dotenv` / `os.getenv()`. Scripts needing migration:
- `airsonic_checker.py` — uses `load_dotenv()` + `os.getenv()`
- `qbittorrent_checker.py` — uses `load_dotenv()` + `os.getenv()`
- `revisor_calendario.py` — uses `os.getenv()`
- `estadisticas/albums_log/cal_to_estadisticas.py` — uses env vars

### 2. Fix `cal_to_estadisticas.py`
Discard "store date" (fecha de tienda), keeping only:
- **Fecha Lanzamiento** — VEVENT in Radicale
- **Fecha Adquisición** — DTSTART of VTODO in Radicale
- **Fecha Escucha** — COMPLETED date of VTODO in Radicale

**Invariant**: `release_date ≤ purchase_date ≤ listen_date`

**Remaster exception**: If `release_date > purchase_date` or `release_date > listen_date`, the album is a remaster of something already owned/heard. In this case, omit purchase and listen dates (leave empty) — the remaster hasn't been listened to yet.

## Radicale CalDAV Structure
- `CALENDAR_NAME` — VEVENT calendar (all-day events = album release dates, summary format: `Artist - Album`)
- `CALENDAR_TASKS` — VTODO calendar (tasks with `DTSTART` = purchase date, `COMPLETED` = listen date)

## External API Rate Limits
- MusicBrainz: 1.1s between requests (required by ToS), set `MB_EMAIL` as user-agent
- Last.fm: 0.25s between album requests
