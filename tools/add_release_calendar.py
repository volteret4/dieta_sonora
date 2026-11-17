#!/usr/bin/env python3
"""Lleva al calendario los lanzamientos de tus artistas (feed de Muspy).

Cada disco es un evento de día completo el día que sale: «💿 Artista - Disco» o
«🎤 …» si es un EP o un single. No repite los que ya están y, al terminar, borra
los duplicados que haya.

Dónde se escribe lo decide CALENDAR_BACKENDS (ver calendario.py): Radicale,
un calendario concreto de Google o los dos.

Variables (.env junto a este script, o .encrypted.env con SOPS):
    MUSPY_FEED_URL          tu feed de Muspy (muspy.com → Settings → feed)
    CALENDAR_BACKENDS       radicale (por defecto), google o radicale,google
    Radicale: RADICALE_URL, RADICALE_USERNAME, RADICALE_PW, RELEASES_CALENDAR_NAME (discos)
    Google:   GOOGLE_CREDENTIALS_FILE, GOOGLE_CALENDAR_ID (y GOOGLE_TOKEN_FILE si es OAuth)
"""
import os
import re
import sys
from datetime import datetime

import feedparser

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    from tools.sops_env import load_sops_env
    load_sops_env()
except (ImportError, FileNotFoundError, RuntimeError):
    pass
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
except ImportError:
    pass

import calendario  # noqa: E402  (después de cargar las variables)

EP_HINTS = ("ep", " - ep", "extended play", "mini album", "single")
DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")


def release_type(title):
    t = title.lower()
    return "EP" if any(h in t for h in EP_HINTS) else "Album"


def parse_feed(url):
    feed = feedparser.parse(url)
    if feed.bozo:
        raise ValueError("El feed no es válido o el XML está roto.")
    out = []
    for entry in feed.entries:
        title = entry.title.strip()
        m = DATE_RE.search(entry.summary)
        if not m:
            print(f"Sin fecha, lo salto: {entry.summary}")
            continue
        try:
            day = datetime.strptime(m.group(), "%Y-%m-%d").date()
        except ValueError:
            print(f"Fecha ilegible, lo salto: {m.group()}")
            continue
        icon = "🎤" if release_type(title) == "EP" else "💿"
        out.append({"title": f"{icon} {title}", "date": day})
    return out


def sync(cal, releases):
    """Añade lo que falta y borra duplicados en un calendario."""
    if not releases:
        return
    start, end = min(r["date"] for r in releases), max(r["date"] for r in releases)
    existing = {(s, d) for s, d, _ in cal.events(start, end)}
    added = 0
    for r in releases:
        key = (r["title"].strip(), r["date"])
        if key in existing:
            continue
        cal.add_all_day(key[0], key[1], f"Release Date for {key[0]}")
        existing.add(key)
        added += 1
        print(f"{cal.label}: añadido {key[0]} ({key[1]})")
    seen, removed = set(), 0
    for summary, day, handle in cal.events(start, end):
        if (summary, day) in seen:
            try:
                cal.delete(handle)
                removed += 1
            except Exception as e:
                print(f"{cal.label}: no pude borrar un duplicado: {e}")
        else:
            seen.add((summary, day))
    print(f"{cal.label}: {added} añadidos, {removed} duplicados borrados")


def main():
    url = os.getenv("MUSPY_FEED_URL")
    if not url:
        raise SystemExit("✗ Falta MUSPY_FEED_URL (tu feed de Muspy)")
    releases = parse_feed(url)
    print(f"{len(releases)} lanzamientos en el feed")
    failed = False
    for backend in calendario.backends():
        try:
            sync(calendario.release_calendar(backend), releases)
        except Exception as e:  # un destino caído no impide escribir en el otro
            failed = True
            print(f"✗ {backend}: {e}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
