#!/usr/bin/env python3
"""Elimina de resultado_flacs.json (y de albums.csv en consecuencia) los
álbumes que ya están en Airsonic o en qBittorrent.

resultado_flacs.json es la fuente real de lo que se muestra en la web
(html_generator.py lee de ahí, no de albums.csv) y solo tiene una vía de
borrado: el botón "eliminar" de la UI, uno a uno. Los pasos diarios
(airsonic_clean_csv.py / qbittorrent_cleaner_csv.py --clean) limpian
albums.csv, pero nunca tocan el JSON -- así que un álbum que ya tenías
cuando se añadió (o que descargaste después, por fuera de esta web) se
queda ahí mostrándose para siempre. Este script hace el mismo chequeo pero
sobre lo que ya está en el JSON, no solo sobre lo que se va a añadir.

Uso:
    python limpiar_ya_tengo.py             # limpia de verdad
    python limpiar_ya_tengo.py --dry-run   # solo muestra qué se eliminaría
"""

import argparse
import csv
import json
import os
import re
import unicodedata

from ya_lo_tengo import qb_torrent_names, ya_lo_tengo

JSON_FILENAME = os.path.join(os.path.dirname(__file__), "resultado_flacs.json")
CSV_FILENAME = os.path.join(os.path.dirname(__file__), "albums.csv")


def _normalize(s: str) -> str:
    s = re.sub(r"\s+", " ", s.strip().lower())
    s = unicodedata.normalize("NFD", s)
    return "".join(c for c in s if unicodedata.category(c) != "Mn")


def _read_csv_types() -> dict[tuple, str]:
    types = {}
    if not os.path.exists(CSV_FILENAME):
        return types
    with open(CSV_FILENAME, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            key = (_normalize(row.get("artist", "")), _normalize(row.get("album", "")))
            types[key] = row.get("type", "vevent")
    return types


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Solo muestra qué se eliminaría")
    args = parser.parse_args()

    if not os.path.exists(JSON_FILENAME):
        print(f"No existe {JSON_FILENAME}, nada que limpiar")
        return

    with open(JSON_FILENAME, encoding="utf-8") as f:
        albums = json.load(f)

    print(f"🔍 Comprobando {len(albums)} álbumes contra Airsonic/qBittorrent...")
    torrent_names = qb_torrent_names()

    conservados, eliminados = [], []
    for i, album in enumerate(albums, 1):
        artist, name = album.get("artist", ""), album.get("album", "")
        print(f"  [{i}/{len(albums)}] {artist} - {name}", end=" ")
        if ya_lo_tengo(artist, name, torrent_names):
            print("⏭️  ya lo tienes, se elimina")
            eliminados.append(album)
        else:
            print("· se conserva")
            conservados.append(album)

    if not eliminados:
        print("\n✅ Nada que limpiar, todos son álbumes nuevos.")
        return

    print(f"\n{len(eliminados)} álbum(es) para eliminar de {len(albums)}:")
    for a in eliminados:
        print(f"  - {a.get('artist')} - {a.get('album')}")

    if args.dry_run:
        print("\n(--dry-run: no se ha escrito nada)")
        return

    with open(JSON_FILENAME, "w", encoding="utf-8") as f:
        json.dump(conservados, f, ensure_ascii=False, indent=2)

    # Mantener albums.csv consistente con lo que sobrevive en el JSON,
    # igual que hace eliminar_grupo_de_datos() en app.py al borrar a mano.
    existing_types = _read_csv_types()
    rows = []
    for a in conservados:
        key = (_normalize(a.get("artist", "")), _normalize(a.get("album", "")))
        rows.append({
            "artist": a.get("artist", ""),
            "album": a.get("album", ""),
            "type": existing_types.get(key, "vevent"),
        })
    with open(CSV_FILENAME, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["artist", "album", "type"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n✅ Eliminados {len(eliminados)} álbum(es). Quedan {len(conservados)}.")


if __name__ == "__main__":
    main()
