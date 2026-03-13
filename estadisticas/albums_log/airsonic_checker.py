#!/usr/bin/env python3
"""
Para cada VEVENT del calendario de lanzamientos que no tenga VTODO asociado
en el calendario de tareas, comprueba si el álbum existe en Airsonic.
Si lo encuentra, crea el VTODO con DTSTART igual a la fecha en que el álbum
fue añadido a la biblioteca de Airsonic.

Uso:
    python airsonic_checker.py              # procesa y crea VTODOs
    python airsonic_checker.py --dry-run    # solo muestra, no escribe
    python airsonic_checker.py --since 365  # solo eventos del último año
"""

import argparse
import os
import re
import sys
import unicodedata
import uuid
from datetime import date, datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import requests
from icalendar import Calendar, Todo
from sops_env import load_sops_env

load_sops_env()

RADICALE_URL         = os.getenv('RADICALE_URL',          '').rstrip('/')
RADICALE_USER        = os.getenv('RADICALE_USERNAME',     '')
RADICALE_PW          = os.getenv('RADICALE_PW',           '')
RADICALE_BASE        = os.getenv('RADICALE_CALENDAR',     '').rstrip('/')
CALENDAR_NAME        = os.getenv('CALENDAR_NAME',         '')
CALENDAR_TASKS       = os.getenv('CALENDAR_TASKS',        '')
AIRSONIC_URL         = os.getenv('AIRSONIC_URL',          '').rstrip('/')
AIRSONIC_USER        = os.getenv('AIRSONIC_USER',         '')
AIRSONIC_PASS        = os.getenv('AIRSONIC_PASS',         '')
AIRSONIC_API_VERSION = os.getenv('AIRSONIC_API_VERSION',  '1.15.0')


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

def _cal_url(cal_name: str) -> str:
    return f'{RADICALE_URL}{RADICALE_BASE}/{cal_name}/'


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


def put_ical(href: str, ical_text: str, cal_name: str) -> bool:
    filename = os.path.basename(href.rstrip('/'))
    url = f'{RADICALE_URL}{RADICALE_BASE}/{cal_name}/{filename}'
    r = requests.put(
        url,
        data=ical_text.encode('utf-8'),
        headers={'Content-Type': 'text/calendar; charset=utf-8'},
        auth=(RADICALE_USER, RADICALE_PW),
        timeout=30,
    )
    if r.status_code not in (200, 201, 204):
        print(f'    ⚠  PUT {url} → HTTP {r.status_code}: {r.text[:120]}')
        return False
    return True


def create_vtodo(artist: str, album: str, purchase_date: date) -> str | None:
    uid = str(uuid.uuid4())
    cal = Calendar()
    cal.add('PRODID', '-//airsonic_checker//ES')
    cal.add('VERSION', '2.0')
    todo = Todo()
    todo.add('UID',     uid)
    todo.add('SUMMARY', f'{artist} - {album}')
    todo.add('DTSTART', purchase_date)
    todo.add('STATUS',  'NEEDS-ACTION')
    todo.add('DTSTAMP', datetime.now(tz=timezone.utc))
    todo.add('CREATED', datetime.now(tz=timezone.utc))
    cal.add_component(todo)
    ical_text = cal.to_ical().decode('utf-8')
    href = f'{RADICALE_BASE}/{CALENDAR_TASKS}/{uid}.ics'
    return href if put_ical(href, ical_text, CALENDAR_TASKS) else None


# ── Airsonic ──────────────────────────────────────────────────────────────────

def search_airsonic(artist: str, album: str) -> date | None:
    """
    Busca el álbum en Airsonic y devuelve la fecha `created` (fecha de añadido
    a la biblioteca), o None si no se encuentra.
    """
    if not AIRSONIC_URL or not AIRSONIC_USER:
        return None

    try:
        r = requests.get(
            f'{AIRSONIC_URL}/rest/search3',
            params={
                'u': AIRSONIC_USER,
                'p': AIRSONIC_PASS,
                'v': AIRSONIC_API_VERSION,
                'c': 'airsonic_checker',
                'f': 'json',
                'query': album,
                'albumCount': 50,
                'albumOffset': 0,
                'artistCount': 0,
                'songCount': 0,
            },
            timeout=10,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f'    ⚠  Airsonic: error de conexión — {e}')
        return None

    if data.get('subsonic-response', {}).get('status') != 'ok':
        print(f'    ⚠  Airsonic: respuesta no OK — {data}')
        return None

    albums = (
        data.get('subsonic-response', {})
            .get('searchResult3', {})
            .get('album', [])
    )
    if not albums:
        return None

    artist_n = _normalize(artist)
    album_n  = _normalize(album)
    best: date | None = None

    for found in albums:
        fa = _normalize(found.get('artist', ''))
        fn = _normalize(found.get('name',   ''))
        if fa != artist_n:
            continue
        if fn != album_n and album_n not in fn:
            continue
        raw = found.get('created', '')
        if not raw:
            return date.today()
        try:
            d = datetime.fromisoformat(raw.rstrip('Z')).date()
        except ValueError:
            d = date.today()
        if best is None or d < best:
            best = d

    return best


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--dry-run', action='store_true',
                        help='Solo muestra, no crea VTODOs')
    parser.add_argument('--since', type=int, default=0, metavar='DÍAS',
                        help='Limitar a VEVENTs de los últimos N días (0 = todos)')
    args = parser.parse_args()

    missing = [v for v in ('RADICALE_URL', 'CALENDAR_NAME', 'CALENDAR_TASKS',
                           'AIRSONIC_URL', 'AIRSONIC_USER')
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

    # Parsear VTODOs → conjunto de claves que ya tienen tarea
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

    # Parsear VEVENTs y cruzar con tareas
    stats = {'sin_tarea': 0, 'en_airsonic': 0, 'creados': 0, 'no_encontrado': 0}

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
            if key in task_keys:
                continue  # ya tiene VTODO

            stats['sin_tarea'] += 1
            print(f'  ❓ Sin tarea: {artist} — {album}  (lanzamiento: {release or "?"})')

            purchase_date = search_airsonic(artist, album)
            if purchase_date is None:
                print(f'     ℹ  No encontrado en Airsonic')
                stats['no_encontrado'] += 1
                continue

            stats['en_airsonic'] += 1
            print(f'     🛒 Airsonic: añadido el {purchase_date.isoformat()}')

            if args.dry_run:
                print(f'     [DRY RUN] crearía VTODO con DTSTART={purchase_date.isoformat()}')
                stats['creados'] += 1
            else:
                href = create_vtodo(artist, album, purchase_date)
                if href:
                    print(f'     ✅ VTODO creado: {href}')
                    task_keys.add(key)
                    stats['creados'] += 1
                else:
                    print(f'     ❌ Error creando VTODO')

    print('\n' + '=' * 50)
    print(f'VEVENTs sin tarea:       {stats["sin_tarea"]}')
    print(f'Encontrados en Airsonic: {stats["en_airsonic"]}')
    print(f'VTODOs creados:          {stats["creados"]}')
    print(f'No en Airsonic:          {stats["no_encontrado"]}')


if __name__ == '__main__':
    main()
