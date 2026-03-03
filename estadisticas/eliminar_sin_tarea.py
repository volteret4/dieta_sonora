#!/usr/bin/env python3
"""
clean_orphans.py — Borra del CSV y del JSON los discos que no tienen VTODO en Radicale.

Flujo:
  1. Descarga todos los VTODOs de Radicale → conjunto de claves (artista, album).
  2. Lee albums.csv y stats.json.
  3. Elimina las filas/entradas que NO aparecen en ese conjunto.
  4. Sobrescribe CSV y JSON con los datos limpios.
  5. Muestra un resumen de lo borrado (--dry-run para no escribir nada).

Uso:
    python clean_orphans.py
    python clean_orphans.py --dry-run          # solo muestra qué borraría
    python clean_orphans.py --csv otra.csv --json otro.json
"""

import argparse
import csv
import json
import os
import re
import sys
import unicodedata
from typing import Optional
from xml.etree import ElementTree as ET

import requests
from dotenv import load_dotenv
from icalendar import Calendar

load_dotenv()

# ─────────────────────────────────────────────
#  CONFIGURACIÓN
# ─────────────────────────────────────────────
RADICALE_URL      = os.getenv("RADICALE_URL", "").rstrip("/")
RADICALE_USER     = os.getenv("RADICALE_USERNAME", "")
RADICALE_PW       = os.getenv("RADICALE_PW", "")
RADICALE_CALENDAR = os.getenv("RADICALE_CALENDAR", "/")

DEFAULT_CSV  = os.getenv("STORE_CSV",  "albums.csv")
DEFAULT_JSON = os.getenv("JSON_PATH",  "stats.json")


# ─────────────────────────────────────────────
#  HELPERS
# ─────────────────────────────────────────────

def _normalize(s: str) -> str:
    """Lowercase + sin acentos + colapsar espacios. Igual que en los otros scripts."""
    s = re.sub(r"\s+", " ", s.strip().lower())
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return s


def strip_emojis(s: str) -> str:
    return re.sub(
        r"^[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+"
        r"|[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+$",
        "", s,
    ).strip()


def parse_summary(summary: str) -> tuple[str, str]:
    """'Artist - Album' → (artist, album)."""
    summary = strip_emojis(summary)
    parts = re.split(r"\s+[-–—]\s+", summary, maxsplit=1)
    if len(parts) == 2:
        return strip_emojis(parts[0]), strip_emojis(parts[1])
    return summary, ""


# ─────────────────────────────────────────────
#  CALDAV — obtener VTODOs
# ─────────────────────────────────────────────

def fetch_vtodo_keys() -> set[tuple[str, str]]:
    """
    Descarga todos los VTODOs de Radicale y devuelve un set de
    (artist_normalized, album_normalized).
    """
    url = RADICALE_URL + RADICALE_CALENDAR
    body = """<?xml version="1.0" encoding="UTF-8"?>
<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">
  <D:prop>
    <D:getetag/>
    <C:calendar-data/>
  </D:prop>
  <C:filter>
    <C:comp-filter name="VCALENDAR">
      <C:comp-filter name="VTODO"/>
    </C:comp-filter>
  </C:filter>
</C:calendar-query>"""

    headers = {"Depth": "1", "Content-Type": "application/xml; charset=utf-8"}
    r = requests.request(
        "REPORT", url,
        data=body.encode("utf-8"),
        headers=headers,
        auth=(RADICALE_USER, RADICALE_PW),
        timeout=30,
    )
    r.raise_for_status()

    ns   = {"D": "DAV:", "C": "urn:ietf:params:xml:ns:caldav"}
    root = ET.fromstring(r.content)

    keys: set[tuple[str, str]] = set()
    for resp in root.findall(".//D:response", ns):
        cal_data = resp.find(".//C:calendar-data", ns)
        if not (cal_data is not None and cal_data.text):
            continue
        try:
            cal = Calendar.from_ical(cal_data.text)
        except Exception as e:
            print(f"  ⚠ Error parseando ítem CalDAV: {e}")
            continue

        for comp in cal.walk():
            if getattr(comp, "name", "") == "VTODO":
                summary = str(comp.get("SUMMARY", ""))
                artist, album = parse_summary(summary)
                if album:
                    keys.add((_normalize(artist), _normalize(album)))

    return keys


# ─────────────────────────────────────────────
#  CSV
# ─────────────────────────────────────────────

def clean_csv(path: str, vtodo_keys: set, dry_run: bool) -> list[dict]:
    """
    Elimina del CSV las filas cuyo (artista, album) no esté en vtodo_keys.
    Devuelve lista de filas borradas.
    """
    if not os.path.exists(path):
        print(f"  ⚠ CSV no encontrado: {path}")
        return []

    with open(path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print(f"  CSV vacío: {path}")
        return []

    fieldnames = list(rows[0].keys())
    kept    = []
    removed = []

    for row in rows:
        # Admite columnas artista/artist y album/álbum
        artist = row.get("artista", row.get("artist", "")).strip()
        album  = row.get("album",   row.get("álbum", "")).strip()
        key    = (_normalize(artist), _normalize(album))
        if key in vtodo_keys:
            kept.append(row)
        else:
            removed.append(row)

    if not removed:
        print(f"  CSV: nada que borrar.")
        return []

    print(f"\n  CSV — {len(removed)} fila(s) a eliminar:")
    for r in removed:
        artist = r.get("artista", r.get("artist", "?"))
        album  = r.get("album",   r.get("álbum", "?"))
        print(f"    - {artist} — {album}")

    if not dry_run:
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(kept)
        print(f"  ✅ CSV actualizado ({len(kept)} entradas).")

    return removed


# ─────────────────────────────────────────────
#  JSON
# ─────────────────────────────────────────────

def clean_json(path: str, vtodo_keys: set, dry_run: bool) -> list[dict]:
    """
    Elimina del JSON los álbumes cuyo (artista, album) no esté en vtodo_keys.
    Devuelve lista de entradas borradas.
    """
    if not os.path.exists(path):
        print(f"  ⚠ JSON no encontrado: {path}")
        return []

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    albums  = data.get("albums", [])
    kept    = []
    removed = []

    for entry in albums:
        artist = (entry.get("artist") or "").strip()
        album  = (entry.get("album")  or "").strip()
        key    = (_normalize(artist), _normalize(album))
        if key in vtodo_keys:
            kept.append(entry)
        else:
            removed.append(entry)

    if not removed:
        print(f"  JSON: nada que borrar.")
        return []

    print(f"\n  JSON — {len(removed)} entrada(s) a eliminar:")
    for e in removed:
        print(f"    - {e.get('artist', '?')} — {e.get('album', '?')}")

    if not dry_run:
        # Limpiar también artistas y géneros huérfanos
        remaining_artist_ids = {e["artist_id"] for e in kept if "artist_id" in e}
        remaining_genre_names = {e.get("genre") for e in kept if e.get("genre")}

        data["albums"] = kept

        if "artists" in data:
            data["artists"] = [
                a for a in data["artists"]
                if a.get("artist_id") in remaining_artist_ids
            ]

        if "genres" in data:
            data["genres"] = [
                g for g in data["genres"]
                if g.get("name") in remaining_genre_names
            ]

        from datetime import datetime
        data["generated_at"] = datetime.now().isoformat()

        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"  ✅ JSON actualizado ({len(kept)} álbumes).")

    return removed


# ─────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Borra del CSV y JSON los discos sin VTODO en Radicale."
    )
    parser.add_argument("--csv",     default=DEFAULT_CSV,  help=f"Ruta al CSV  (defecto: {DEFAULT_CSV})")
    parser.add_argument("--json",    default=DEFAULT_JSON, help=f"Ruta al JSON (defecto: {DEFAULT_JSON})")
    parser.add_argument("--dry-run", action="store_true",  help="Solo muestra qué borraría, sin escribir nada")
    args = parser.parse_args()

    print("🧹 clean_orphans.py — Limpieza de discos sin VTODO")
    if args.dry_run:
        print("   [DRY RUN — no se escribirá nada]")
    print("=" * 55)

    if not RADICALE_URL or not RADICALE_USER:
        print("❌ Configura RADICALE_URL, RADICALE_USERNAME y RADICALE_PW en .env")
        sys.exit(1)

    # 1. Obtener VTODOs activos
    print("\n📅 Descargando VTODOs de Radicale...")
    try:
        vtodo_keys = fetch_vtodo_keys()
    except Exception as e:
        print(f"  ❌ Error CalDAV: {e}")
        sys.exit(1)
    print(f"  {len(vtodo_keys)} VTODOs encontrados.")

    # 2. Limpiar CSV
    print(f"\n📋 Procesando CSV: {args.csv}")
    removed_csv  = clean_csv(args.csv,  vtodo_keys, args.dry_run)

    # 3. Limpiar JSON
    print(f"\n📊 Procesando JSON: {args.json}")
    removed_json = clean_json(args.json, vtodo_keys, args.dry_run)

    # 4. Resumen
    print("\n" + "=" * 55)
    print("📊 Resumen:")
    print(f"  Eliminados del CSV:  {len(removed_csv)}")
    print(f"  Eliminados del JSON: {len(removed_json)}")
    if args.dry_run:
        print("\n  (dry-run: no se ha modificado ningún fichero)")
    else:
        print("\n✅ ¡Hecho! Vuelve a abrir estadisticas.html para ver los cambios.")


if __name__ == "__main__":
    main()
