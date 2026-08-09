# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Subdirectory Purpose

This directory (`estadisticas/albums_log`) handles syncing Radicale CalDAV data into SQLite + JSON for the statistics web dashboard. See the parent `CLAUDE.md` for overall project architecture.

Tracks **Release → Listened** only. Purchase/store-date tracking was removed:
estimating it from Airsonic's `created` (library scan time, not acquisition
time) or qBittorrent's `added_on` produced too many nonsensical/negative
date chains to be worth keeping.

## Scripts

| Script | Purpose |
|---|---|
| `cal_to_estadisticas.py` | Main sync script: VTODO-driven, updates `music_stats.db`, marks COMPLETED via Last.fm scrobble matching |
| `airsonic_checker.py` | Creates anchor VTODOs (no date) for VEVENTs without a task, if the album is found in Airsonic — just to get it into the tracking loop |
| `qbittorrent_checker.py` | Same as above but checks qBittorrent |
| `extraer_estadisticas.py` | Older extraction script (reads both VEVENT+VTODO, fetches genres from MB/Last.fm); still used to populate genre data |
| `sops_env.py` | SOPS+age bridge replacing `python-dotenv`; walks up directories to find `.encrypted.env` |

## Running Scripts

```bash
# Decrypt secrets first (or use sops_env.py which does it automatically)
sops -d .encrypted.env

# Sync all VTODOs to DB (source of truth)
python cal_to_estadisticas.py --auto

# Dry-run
python cal_to_estadisticas.py --dry-run

# Detect albums in Airsonic without a VTODO, create anchor VTODOs
python airsonic_checker.py --since 365 --dry-run

# Detect albums in qBittorrent without a VTODO, create anchor VTODOs
python qbittorrent_checker.py --since 365 --dry-run

# Enrich genres from MusicBrainz/Last.fm (older script, also does its own
# CalDAV fetch — only worth running standalone, main.sh always uses
# --export-only since cal_to_estadisticas.py already left the DB current)
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
- `albums(album_id, artist_id, genre_id, name, name_normalized, release_date, listened_date, days_release_to_listened)`

Older deployed DBs may still carry orphaned `purchase_date`,
`purchase_date_estimated`, `days_release_to_purchase`,
`days_purchase_to_listened` columns from before purchase-tracking was
removed — `init_db()` in both `cal_to_estadisticas.py` and
`extraer_estadisticas.py` leaves them in place (dropping columns is riskier
than ignoring them) and additively migrates in `days_release_to_listened`.

`lastfm_stats.db` — read-only input; must exist for listen-date detection. Schema: `artists(artist_id, name_normalized)`, `scrobbles(artist_id, track_normalized, ts, ts_iso)`. Mounted `:ro` — open with `mode=ro&immutable=1` (see `cal_to_estadisticas.py`), plain `mode=ro` isn't enough because the DB is in WAL mode and even a reader needs to touch the `-shm` file.

## Key Invariants

- **Date chain**: `release_date ≤ listened_date` — `_sanitize_chain()` in `cal_to_estadisticas.py` drops a `listened_date` earlier than `release_date` (treated as a bad Last.fm match, e.g. an earlier edition's scrobble).
- **Dedup key**: `(_normalize(artist), _normalize(album))` — NFD + lowercase + collapse whitespace + strip combining marks
- **VTODO is source of truth**: `cal_to_estadisticas.py` iterates VTODOs, not VEVENTs; VEVENTs are only cross-referenced for `release_date`. A VEVENT with no VTODO at all never enters the tracking loop — that's what `airsonic_checker.py`/`qbittorrent_checker.py` are for (anchor-VTODO creation).

## CalDAV Write Operations

- `put_ical()` rebuilds the URL using `cal_name` to avoid Radicale 403s from UUID-based internal paths
- VTODO `COMPLETED` updates set `STATUS=COMPLETED` + add `COMPLETED` property with UTC datetime
- **Never rebuild a `Calendar` by iterating `cal.walk()` and re-adding each component to a new `Calendar()`** — `walk()` also yields nested subcomponents (e.g. `VALARM`, which DAVx5/Tasks.org always attaches to its VTODOs), so that pattern duplicates them as top-level siblings instead of leaving them nested. Radicale then rejects the PUT with `HTTP 400`, silently breaking updates to any real user-created task. Use `_find_vtodo(cal)` to locate the VTODO, mutate it in place, and call `cal.to_ical()` on the original (mutated) `Calendar` instead.
