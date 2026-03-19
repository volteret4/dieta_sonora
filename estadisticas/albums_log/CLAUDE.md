# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Subdirectory Purpose

This directory (`estadisticas/albums_log`) handles syncing Radicale CalDAV data into SQLite + JSON for the statistics web dashboard. See the parent `CLAUDE.md` for overall project architecture.

## Scripts

| Script | Purpose |
|---|---|
| `cal_to_estadisticas.py` | Main sync script: VTODO-driven, updates `music_stats.db`, marks COMPLETED via Last.fm scrobble matching |
| `airsonic_checker.py` | Creates VTODOs for VEVENTs without tasks, using Airsonic `created` date as purchase date |
| `qbittorrent_checker.py` | Same as above but uses qBittorrent `added_on` date |
| `extraer_estadisticas.py` | Older extraction script (reads both VEVENT+VTODO, fetches genres from MB/Last.fm); still used to populate genre data |
| `sops_env.py` | SOPS+age bridge replacing `python-dotenv`; walks up directories to find `.encrypted.env` |

## Running Scripts

```bash
# Decrypt secrets first (or use sops_env.py which does it automatically)
sops -d .encrypted.env

# Sync all VTODOs to DB (source of truth)
python cal_to_estadisticas.py --all-data

# Sync only recent (last 7 days), dry-run
python cal_to_estadisticas.py --since 7 --dry-run

# Detect albums in Airsonic without a VTODO, create VTODOs
python airsonic_checker.py --since 365 --dry-run

# Detect albums in qBittorrent without a VTODO, create VTODOs
python qbittorrent_checker.py --since 365 --dry-run

# Enrich genres from MusicBrainz/Last.fm (older script)
python extraer_estadisticas.py
```

## Secret Loading

- `airsonic_checker.py` and `qbittorrent_checker.py`: use `sops_env.load_sops_env()` (already migrated)
- `cal_to_estadisticas.py` and `extraer_estadisticas.py`: still use `load_dotenv()` (pending migration per parent CLAUDE.md)
- `sops_env.py` searches upward from `Path.cwd()` for `.encrypted.env`, so it finds the root file automatically

## Database Schema

`music_stats.db` (SQLite):
- `artists(artist_id, name, name_normalized)`
- `genres(genre_id, name, name_normalized)`
- `artist_genres(artist_id, genre_id)` — many-to-many
- `albums(album_id, artist_id, genre_id, name, name_normalized, release_date, purchase_date, listened_date, days_release_to_purchase, days_purchase_to_listened)`

`lastfm_stats.db` — read-only input; must exist for listen-date detection. Schema: `artists(artist_id, name_normalized)`, `scrobbles(artist_id, track_normalized, ts, ts_iso)`.

## Key Invariants

- **Date chain**: `release_date ≤ purchase_date ≤ listened_date`
- **Remaster rule**: if `release_date > purchase_date` or `release_date > listen_date`, it's a remaster — `_sanitize_chain()` in `cal_to_estadisticas.py` drops purchase/listen dates automatically
- **Dedup key**: `(_normalize(artist), _normalize(album))` — NFD + lowercase + collapse whitespace + strip combining marks
- **VTODO is source of truth**: `cal_to_estadisticas.py` iterates VTODOs, not VEVENTs; VEVENTs are only cross-referenced for `release_date`

## CalDAV Write Operations

- `put_ical()` rebuilds the URL using `cal_name` to avoid Radicale 403s from UUID-based internal paths
- VTODO `COMPLETED` updates set `STATUS=COMPLETED` + add `COMPLETED` property with UTC datetime
- Missing `DTSTART`: calculated as `DUE − 3 months`; missing `DUE`: calculated as `DTSTART + 3 months`
