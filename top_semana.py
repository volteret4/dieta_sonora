#!/usr/bin/env python3
"""
Obtiene los discos más descargados de la semana en Orpheus y los añade
a resultado_flacs.json y albums.csv (si no están ya).

Uso:
    python top_semana.py              # top 25 de la semana
    python top_semana.py --limit 50   # ampliar el top
    python top_semana.py --dry-run    # solo muestra, no escribe
"""

import argparse
import csv
import json
import os
import time
from tools.sops_env import load_sops_env

import requests

try:
    load_sops_env()
except FileNotFoundError:
    # En Docker las variables llegan ya descifradas vía docker-compose (scripts/up.sh)
    pass

API_KEY      = os.getenv("ORPHEUS_APIKEY")
BASE_URL     = "https://orpheus.network/ajax.php"
JSON_FILE    = os.path.join(os.path.dirname(__file__), "resultado_flacs.json")
CSV_FILE     = os.path.join(os.path.dirname(__file__), "albums.csv")
HEADERS      = {"Authorization": API_KEY}


# ── Orpheus API ───────────────────────────────────────────────────────────────

def obtener_keys() -> tuple[str | None, str | None]:
    try:
        r = requests.get(BASE_URL, headers=HEADERS,
                         params={"action": "index"}, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("status") != "success":
            print(f"❌ API error: {data.get('error')}")
            return None, None
        return data["response"]["authkey"], data["response"]["passkey"]
    except Exception as e:
        print(f"❌ Error obteniendo llaves: {e}")
        return None, None


def obtener_top_semana(limit: int) -> list[dict]:
    """Devuelve lista de {artist, album, groupId, cover} del top semanal."""
    try:
        r = requests.get(BASE_URL, headers=HEADERS, params={
            "action": "top10",
            "type":   "week",
            "limit":  limit,
        }, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"❌ Error obteniendo top10: {e}")
        return []

    if data.get("status") != "success":
        print(f"❌ API respondió error: {data.get('error')}")
        return []

    seen_groups: set = set()
    entries = []
    for section in data.get("response", []):
        for item in section.get("results", []):
            if not isinstance(item, dict):
                continue
            artist   = (item.get("artist") or "").strip()
            album    = (item.get("groupName") or "").strip()
            group_id = item.get("groupID")
            if artist and album and group_id and group_id not in seen_groups:
                seen_groups.add(group_id)
                entries.append({
                    "artist":  artist,
                    "album":   album,
                    "groupId": group_id,
                    "cover":   item.get("wikiImage", ""),
                })
    return entries


def buscar_grupo(group_id: int, authkey: str, passkey: str) -> dict | None:
    """Obtiene datos completos de un grupo (torrents FLAC) por groupId."""
    try:
        r = requests.get(BASE_URL, headers=HEADERS, params={
            "action": "torrentgroup",
            "id":     group_id,
        }, timeout=10)
        r.raise_for_status()
        data = r.json()
        response = data.get("response")
        if not isinstance(response, dict):
            return None
        return response
    except Exception as e:
        print(f"  ⚠️  Error obteniendo grupo {group_id}: {e}")
        return None


# ── CSV helpers ───────────────────────────────────────────────────────────────

def _leer_csv() -> list[dict]:
    if not os.path.exists(CSV_FILE):
        return []
    with open(CSV_FILE, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader) if reader.fieldnames else []


def _escribir_csv(rows: list[dict]):
    fieldnames = list(dict.fromkeys(k for r in rows for k in r))
    if "type" in fieldnames:
        fieldnames.remove("type")
        fieldnames.append("type")
    with open(CSV_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--limit", type=int, default=25,
                        help="Número de entradas del top a procesar (default: 25)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo muestra qué añadiría, sin escribir nada")
    args = parser.parse_args()

    if not API_KEY:
        print("❌ ORPHEUS_APIKEY no encontrada en el entorno")
        return

    authkey, passkey = obtener_keys()
    if not authkey:
        return

    # Cargar JSON existente para deduplicar
    resultados: list[dict] = []
    if os.path.exists(JSON_FILE):
        with open(JSON_FILE, encoding="utf-8") as f:
            resultados = json.load(f)
    ya_en_json: set[str] = {
        f"{r['artist'].lower()}|{r['album'].lower()}" for r in resultados
    }

    print(f"🏆 Obteniendo top {args.limit} de la semana en Orpheus...")
    top = obtener_top_semana(args.limit)
    print(f"   {len(top)} entradas recibidas\n")

    csv_rows   = _leer_csv()
    ya_en_csv  = {
        f"{r.get('artist','').lower()}|{r.get('album','').lower()}"
        for r in csv_rows
    }

    nuevos_json = 0
    nuevos_csv  = 0

    for item in top:
        artist  = item["artist"]
        album   = item["album"]
        key     = f"{artist.lower()}|{album.lower()}"

        print(f"  🎵 {artist} — {album}")

        # ── Añadir al JSON ────────────────────────────────────────────────────
        if key not in ya_en_json:
            group_id = item["groupId"]
            if not group_id:
                print("     ⚠️  Sin groupId, saltando")
                continue

            time.sleep(1.5)
            grupo_data = buscar_grupo(group_id, authkey, passkey)
            if not grupo_data:
                print("     ⚠️  Sin datos del grupo")
                continue

            torrents = grupo_data.get("torrents", [])
            flacs    = [t for t in torrents if t.get("format") == "FLAC"]
            if not flacs:
                print("     — Sin FLACs")
                continue

            grupo_info = {
                "groupId":  group_id,
                "cover":    item["cover"],
                "webUrl":   f"https://orpheus.network/torrents.php?id={group_id}",
                "flacCount": len(flacs),
                "torrents": [
                    {
                        "torrentId":    t["id"],
                        "media":        t.get("media"),
                        "encoding":     t.get("encoding"),
                        "remasterYear": t.get("remasterYear"),
                        "remasterTitle": t.get("remasterTitle"),
                        "fileCount":    t.get("fileCount"),
                        "size":         t.get("size"),
                        "downloadUrl":  (
                            f"https://orpheus.network/torrents.php"
                            f"?action=download&id={t['id']}"
                            f"&authkey={authkey}&torrent_pass={passkey}&usetoken=1"
                        ),
                    }
                    for t in flacs
                ],
            }

            entrada = {"artist": artist, "album": album, "groups": [grupo_info]}
            if not args.dry_run:
                resultados.append(entrada)
                ya_en_json.add(key)
                with open(JSON_FILE, "w", encoding="utf-8") as f:
                    json.dump(resultados, f, indent=4, ensure_ascii=False)
            nuevos_json += 1
            print(f"     ✅ Añadido al JSON ({len(flacs)} FLACs)")
        else:
            print("     — Ya en JSON")

        # ── Añadir al CSV ─────────────────────────────────────────────────────
        if key not in ya_en_csv:
            row = {"artist": artist, "album": album, "type": "top10"}
            if not args.dry_run:
                csv_rows.append(row)
                ya_en_csv.add(key)
                _escribir_csv(csv_rows)
            nuevos_csv += 1
            print(f"     📋 Añadido al CSV")

        time.sleep(0.5)

    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}✅ Nuevos en JSON: {nuevos_json} | Nuevos en CSV: {nuevos_csv}")


if __name__ == "__main__":
    main()
