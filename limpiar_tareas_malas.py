#!/usr/bin/env python3
"""
Elimina VTODOs cuya fecha de compra (DTSTART) coincide con la fecha de
lanzamiento del álbum (VEVENT DTSTART). Estas tareas fueron creadas
automáticamente con la fecha de lanzamiento como sustituto de la fecha
de compra, lo cual es incorrecto.

Uso:
    python limpiar_tareas_malas.py            # muestra y elimina
    python limpiar_tareas_malas.py --dry-run  # solo muestra
"""

import argparse
import os
import re
import sys
import unicodedata
from xml.etree import ElementTree as ET

import requests
from icalendar import Calendar
from tools.sops_env import load_sops_env

load_sops_env()

RADICALE_URL   = os.getenv('RADICALE_URL',      '').rstrip('/')
RADICALE_USER  = os.getenv('RADICALE_USERNAME', '')
RADICALE_PW    = os.getenv('RADICALE_PW',       '')
RADICALE_BASE  = os.getenv('RADICALE_CALENDAR', '').rstrip('/')
CALENDAR_NAME  = os.getenv('CALENDAR_NAME',  '')
CALENDAR_TASKS = os.getenv('CALENDAR_TASKS', '')


def _cal_url(cal_name: str) -> str:
    return f'{RADICALE_URL}{RADICALE_BASE}/{cal_name}/'


def _normalize(s: str) -> str:
    s = re.sub(r'\s+', ' ', s.strip().lower())
    s = unicodedata.normalize('NFD', s)
    return ''.join(c for c in s if unicodedata.category(c) != 'Mn')


def _strip_emojis(s: str) -> str:
    return re.sub(
        r'^[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+'
        r'|[\U00010000-\U0010ffff\u2000-\u2bff\u2600-\u26ff\u2700-\u27bf\s]+$',
        '', s,
    ).strip()


def _parse_summary(summary: str) -> tuple[str, str]:
    summary = _strip_emojis(summary)
    parts = re.split(r'\s+[-–—]\s+', summary, maxsplit=1)
    if len(parts) == 2:
        return _strip_emojis(parts[0]), _strip_emojis(parts[1])
    return summary, ''


def _parse_date(dt_val) -> str | None:
    if dt_val is None:
        return None
    if hasattr(dt_val, 'dt'):
        dt_val = dt_val.dt
    if hasattr(dt_val, 'date'):
        return dt_val.date().isoformat()
    if hasattr(dt_val, 'isoformat'):
        return dt_val.isoformat()
    return None


def fetch_calendar_items(cal_name: str) -> list[dict]:
    url = _cal_url(cal_name)
    body = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<C:calendar-query xmlns:D="DAV:" xmlns:C="urn:ietf:params:xml:ns:caldav">'
        '  <D:prop><D:getetag/><C:calendar-data/></D:prop>'
        '  <C:filter><C:comp-filter name="VCALENDAR"/></C:filter>'
        '</C:calendar-query>'
    )
    r = requests.request(
        'REPORT', url,
        data=body.encode('utf-8'),
        headers={'Depth': '1', 'Content-Type': 'application/xml; charset=utf-8'},
        auth=(RADICALE_USER, RADICALE_PW),
        timeout=30,
    )
    r.raise_for_status()
    ns = {'D': 'DAV:', 'C': 'urn:ietf:params:xml:ns:caldav'}
    root = ET.fromstring(r.content)
    items = []
    for resp in root.findall('.//D:response', ns):
        href_el  = resp.find('D:href', ns)
        cal_data = resp.find('.//C:calendar-data', ns)
        if href_el is not None and cal_data is not None and cal_data.text:
            items.append({'href': href_el.text, 'ical_text': cal_data.text})
    return items


def delete_item(href: str, cal_name: str) -> bool:
    filename = os.path.basename(href.rstrip('/'))
    url = f'{RADICALE_URL}{RADICALE_BASE}/{cal_name}/{filename}'
    r = requests.delete(url, auth=(RADICALE_USER, RADICALE_PW), timeout=15)
    return r.status_code in (200, 204)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', action='store_true',
                        help='Solo muestra, no elimina nada')
    args = parser.parse_args()

    missing = [v for v in ('RADICALE_URL', 'CALENDAR_NAME', 'CALENDAR_TASKS')
               if not os.getenv(v)]
    if missing:
        print(f'❌ Variables faltantes: {", ".join(missing)}')
        sys.exit(1)

    print('📅 Descargando VEVENTs (lanzamientos)...')
    raw_events = fetch_calendar_items(CALENDAR_NAME)
    print(f'   {len(raw_events)} eventos')

    print('📋 Descargando VTODOs (tareas)...')
    raw_tasks = fetch_calendar_items(CALENDAR_TASKS)
    print(f'   {len(raw_tasks)} tareas\n')

    # Construir mapa VEVENT: (artist_norm, album_norm) → release_date_iso
    release_dates: dict[tuple, str] = {}
    for item in raw_events:
        try:
            cal = Calendar.from_ical(item['ical_text'])
        except Exception:
            continue
        for comp in cal.walk():
            if getattr(comp, 'name', '') != 'VEVENT':
                continue
            artist, album = _parse_summary(str(comp.get('SUMMARY', '')))
            if not album:
                continue
            release = _parse_date(comp.get('DTSTART'))
            if release:
                release_dates[(_normalize(artist), _normalize(album))] = release

    # Revisar VTODOs
    deleted = 0
    kept    = 0

    for item in raw_tasks:
        try:
            cal = Calendar.from_ical(item['ical_text'])
        except Exception:
            continue
        for comp in cal.walk():
            if getattr(comp, 'name', '') != 'VTODO':
                continue
            artist, album = _parse_summary(str(comp.get('SUMMARY', '')))
            if not album:
                continue

            dtstart = _parse_date(comp.get('DTSTART'))
            if dtstart is None:
                kept += 1
                continue

            key = (_normalize(artist), _normalize(album))
            release = release_dates.get(key)

            if release and dtstart == release:
                print(f'  🗑  {artist} — {album}')
                print(f'      DTSTART={dtstart}  ==  lanzamiento={release}')
                if args.dry_run:
                    print('      [DRY RUN] no se elimina')
                    deleted += 1
                else:
                    ok = delete_item(item['href'], CALENDAR_TASKS)
                    print(f'      {"✅ eliminado" if ok else "❌ error al eliminar"}')
                    if ok:
                        deleted += 1
            else:
                kept += 1

    print(f'\n{"[DRY RUN] " if args.dry_run else ""}Eliminadas: {deleted} | Conservadas: {kept}')


if __name__ == '__main__':
    main()
