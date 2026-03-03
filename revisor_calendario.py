#!/usr/bin/env python3
"""
Script para exportar eventos de Radicale a CSV
Formato esperado de eventos: $artist - $album
"""

import argparse
import csv
from datetime import datetime, timedelta
import caldav
from caldav.elements import dav
import sys
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde archivo .env
load_dotenv()

# CONFIGURACIÓN - Ajusta estos valores según tu instalación
RADICALE_URL = os.getenv('RADICALE_URL', 'http://localhost:5232')
RADICALE_USER = os.getenv('RADICALE_USERNAME', '')
RADICALE_PASS = os.getenv('RADICALE_PW', '')
CALENDAR_NAME = os.getenv('CALENDAR_NAME', 'calendar')


def parse_event_summary(summary):
    """
    Parse el formato '$artist - $album' del resumen del evento.
    Omite emojis/iconos al inicio (💿, 🎤, etc.).
    Retorna (artist, album) o None si el formato no coincide.
    """
    if not summary:
        return None

    # Eliminar emojis y caracteres al inicio (cualquier carácter no ASCII seguido de espacio)
    cleaned = summary.strip()

    # Remover emojis comunes al inicio
    while cleaned and ord(cleaned[0]) > 127:  # Caracteres no ASCII (emojis)
        cleaned = cleaned[1:].strip()

    if not cleaned or ' - ' not in cleaned:
        return None

    parts = cleaned.split(' - ', 1)
    if len(parts) != 2:
        return None

    artist = parts[0].strip()
    album = parts[1].strip()

    if not artist or not album:
        return None

    return (artist, album)


def get_events_from_radicale(since_days=0):
    """
    Obtiene eventos de Radicale desde hace 'since_days' días hasta hoy.
    """
    try:
        # Conectar al servidor CalDAV
        if RADICALE_USER and RADICALE_PASS:
            client = caldav.DAVClient(
                url=RADICALE_URL,
                username=RADICALE_USER,
                password=RADICALE_PASS
            )
        else:
            client = caldav.DAVClient(url=RADICALE_URL)

        # Obtener el principal (usuario)
        principal = client.principal()

        # Obtener calendarios
        calendars = principal.calendars()

        if not calendars:
            print("No se encontraron calendarios", file=sys.stderr)
            return []

        # Buscar el calendario específico o usar el primero
        calendar = None
        for cal in calendars:
            cal_name = cal.name
            if cal_name and CALENDAR_NAME in cal_name:
                calendar = cal
                break

        if not calendar:
            calendar = calendars[0]
            print(f"Usando calendario: {calendar.name}", file=sys.stderr)

        # Calcular rango de fechas
        end_date = datetime.now().replace(hour=23, minute=59, second=59)
        start_date = end_date - timedelta(days=since_days)
        start_date = start_date.replace(hour=0, minute=0, second=0)

        print(f"Buscando eventos desde {start_date.date()} hasta {end_date.date()}", file=sys.stderr)

        # Buscar eventos en el rango
        events = calendar.search(start=start_date, end=end_date)

        releases = []

        for event in events:
            try:
                # Obtener el componente del evento
                vevent = event.vobject_instance.vevent

                # Obtener el resumen (título)
                summary = str(vevent.summary.value) if hasattr(vevent, 'summary') else None

                if summary:
                    parsed = parse_event_summary(summary)
                    if parsed:
                        releases.append(parsed)
                        print(f"Encontrado: {parsed[0]} - {parsed[1]}", file=sys.stderr)
                    else:
                        print(f"Formato no válido: {summary}", file=sys.stderr)
            except Exception as e:
                print(f"Error procesando evento: {e}", file=sys.stderr)
                continue

        return releases

    except Exception as e:
        print(f"Error conectando a Radicale: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description='Exporta eventos de Radicale a CSV (formato: artist,album)'
    )
    parser.add_argument(
        '--since',
        type=int,
        default=0,
        help='Número de días hacia atrás para buscar eventos (default: 0, solo hoy)'
    )
    parser.add_argument(
        '-o', '--output',
        default='albums.csv',
        help='Archivo CSV de salida (default: albums.csv)'
    )

    args = parser.parse_args()

    # Leer CSV existente si existe
    existing_releases = []
    if os.path.exists(args.output):
        print(f"Leyendo CSV existente: {args.output}", file=sys.stderr)
        try:
            with open(args.output, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.reader(csvfile)
                for row in reader:
                    if len(row) >= 2:
                        existing_releases.append((row[0], row[1]))
            print(f"Encontrados {len(existing_releases)} lanzamientos existentes", file=sys.stderr)
        except Exception as e:
            print(f"Error leyendo CSV existente: {e}", file=sys.stderr)

    # Obtener nuevos eventos del calendario
    new_releases = get_events_from_radicale(since_days=args.since)

    if not new_releases:
        print("No se encontraron eventos nuevos con el formato esperado", file=sys.stderr)
        if not existing_releases:
            sys.exit(0)
        print("Manteniendo lanzamientos existentes", file=sys.stderr)
    else:
        print(f"\nEncontrados {len(new_releases)} eventos nuevos del calendario", file=sys.stderr)

    # Combinar lanzamientos existentes + nuevos
    all_releases = existing_releases + new_releases

    # Eliminar duplicados manteniendo el orden (primero aparecidos se mantienen)
    seen = set()
    unique_releases = []
    for artist, album in all_releases:
        key = (artist.lower(), album.lower())  # Comparar en minúsculas
        if key not in seen:
            seen.add(key)
            unique_releases.append((artist, album))

    # Escribir al CSV
    with open(args.output, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        for artist, album in unique_releases:
            writer.writerow([artist, album])

    duplicates_removed = len(all_releases) - len(unique_releases)
    new_added = len(unique_releases) - len(existing_releases)

    print(f"\n=== Resumen ===", file=sys.stderr)
    print(f"Lanzamientos previos: {len(existing_releases)}", file=sys.stderr)
    print(f"Eventos nuevos encontrados: {len(new_releases)}", file=sys.stderr)
    print(f"Nuevos añadidos al CSV: {new_added}", file=sys.stderr)
    print(f"Duplicados eliminados: {duplicates_removed}", file=sys.stderr)
    print(f"Total en {args.output}: {len(unique_releases)}", file=sys.stderr)


if __name__ == '__main__':
    main()
