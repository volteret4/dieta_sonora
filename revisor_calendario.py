#!/usr/bin/env python3
"""
Genera albums.csv con los álbumes que tienen VEVENT en el calendario de
lanzamientos pero no tienen VTODO asociado en el calendario de tareas.
El CSV es un snapshot limpio: cada ejecución lo sobreescribe por completo.

Uso:
    python revisor_calendario.py              # todos los VEVENTs pendientes
    python revisor_calendario.py --since 365  # solo VEVENTs de los últimos N días
    python revisor_calendario.py -o salida.csv
"""

import argparse
import csv
import os
import re
import sys
import unicodedata
from datetime import date, datetime, timedelta
from xml.etree import ElementTree as ET

import requests
from icalendar import Calendar
from sops_env import load_sops_env

load_sops_env()

RADICALE_URL   = os.getenv('RADICALE_URL',      '').rstrip('/')
RADICALE_USER  = os.getenv('RADICALE_USERNAME', '')
RADICALE_PW    = os.getenv('RADICALE_PW',       '')
RADICALE_BASE  = os.getenv('RADICALE_CALENDAR', '').rstrip('/')
CALENDAR_NAME  = os.getenv('CALENDAR_NAME',     '')
CALENDAR_TASKS = os.getenv('CALENDAR_TASKS',    '')


# ── Helpers ───────────────────────────────────────────────────────────────────

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


def _parse_date(dt_val) -> date | None:
    if dt_val is None:
        return None
    if hasattr(dt_val, 'dt'):
        dt_val = dt_val.dt
    if isinstance(dt_val, datetime):
        return dt_val.date()
    if isinstance(dt_val, date):
        return dt_val
    return None


# ── CalDAV ────────────────────────────────────────────────────────────────────

def fetch_calendar_items(cal_name: str) -> list[dict]:
    url = f'{RADICALE_URL}{RADICALE_BASE}/{cal_name}/'
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


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--since', type=int, default=0, metavar='DÍAS',
                        help='Limitar a VEVENTs de los últimos N días (0 = todos)')
    parser.add_argument('-o', '--output', default='albums.csv',
                        help='Archivo CSV de salida (default: albums.csv)')
    args = parser.parse_args()

    missing = [v for v in ('RADICALE_URL', 'CALENDAR_NAME', 'CALENDAR_TASKS')
               if not os.getenv(v)]
    if missing:
        print(f'❌ Variables faltantes: {", ".join(missing)}')
        sys.exit(1)

    since_date = date.min if args.since == 0 else date.today() - timedelta(days=args.since)

    print(f'📅 Descargando VEVENTs{f" (últimos {args.since} días)" if args.since else ""}...')
    raw_events = fetch_calendar_items(CALENDAR_NAME)
    print(f'   {len(raw_events)} eventos')

    print('📋 Descargando VTODOs...')
    raw_tasks = fetch_calendar_items(CALENDAR_TASKS)
    print(f'   {len(raw_tasks)} tareas\n')

    # Construir conjunto de claves que ya tienen VTODO
    task_keys: set[tuple] = set()
    for item in raw_tasks:
        try:
            cal = Calendar.from_ical(item['ical_text'])
        except Exception:
            continue
        for comp in cal.walk():
            if getattr(comp, 'name', '') != 'VTODO':
                continue
            artist, album = _parse_summary(str(comp.get('SUMMARY', '')))
            if album:
                task_keys.add((_normalize(artist), _normalize(album)))

    # Filtrar VEVENTs sin VTODO
    pending: list[tuple[str, str]] = []
    seen: set[tuple] = set()
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
            if release and release < since_date:
                continue

            key = (_normalize(artist), _normalize(album))
            if key in task_keys or key in seen:
                continue

            seen.add(key)
            pending.append((artist, album))

    # Escribir CSV (snapshot limpio)
    with open(args.output, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['artist', 'album', 'type'])
        writer.writeheader()
        for artist, album in pending:
            writer.writerow({'artist': artist, 'album': album, 'type': 'vevent'})

    print(f'✅ {len(pending)} álbumes pendientes escritos en {args.output}')


if __name__ == '__main__':
    main()
