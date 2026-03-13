#!/usr/bin/env python3
"""
Script para verificar qué álbumes del CSV ya existen en Airsonic
"""

import csv
import hashlib
import requests
import argparse
import sys
import os
import unicodedata
from urllib.parse import urljoin
from sops_env import load_sops_env

# Cargar variables de entorno desde .env
load_sops_env()

# CONFIGURACIÓN - Ajusta estos valores según tu instalación
AIRSONIC_URL = os.getenv('AIRSONIC_URL', 'http://localhost:4040')
AIRSONIC_USER = os.getenv('AIRSONIC_USER', 'admin')
AIRSONIC_PASS = os.getenv('AIRSONIC_PASS', 'admin')
AIRSONIC_API_VERSION = '1.15.0'
# Método de autenticación: 'password' (texto plano) o 'token' (MD5)
# La mayoría de servidores Airsonic requieren 'password'
AIRSONIC_AUTH_METHOD = os.getenv('AIRSONIC_AUTH_METHOD', 'password')


def normalize_text(text):
    """
    Normaliza texto eliminando acentos y caracteres especiales.
    Convierte a minúsculas y elimina espacios extra.

    Ejemplos:
    - "José González" -> "jose gonzalez"
    - "Sigur Rós" -> "sigur ros"
    - "Björk" -> "bjork"
    """
    if not text:
        return ""

    # Normalizar unicode (descomponer caracteres acentuados)
    # NFD = Normalization Form Decomposed
    # Por ejemplo: 'é' se convierte en 'e' + '´'
    nfd = unicodedata.normalize('NFD', text)

    # Filtrar solo caracteres ASCII (elimina los acentos)
    # Por ejemplo: 'e' + '´' -> 'e'
    sin_acentos = ''.join(
        char for char in nfd
        if unicodedata.category(char) != 'Mn'  # Mn = Mark, Nonspacing (acentos)
    )

    # Convertir a minúsculas y limpiar espacios
    normalizado = sin_acentos.lower().strip()

    # Reemplazar múltiples espacios por uno solo
    normalizado = ' '.join(normalizado.split())

    return normalizado


def generate_token(password, salt):
    """
    Genera el token de autenticación para Airsonic.
    token = md5(password + salt)
    """
    token_string = password + salt
    return hashlib.md5(token_string.encode('utf-8')).hexdigest()


def search_album_in_airsonic(artist, album):
    """
    Busca un álbum específico en Airsonic usando la API search3.
    Retorna True si se encuentra, False si no.
    """
    try:
        # Preparar parámetros base
        params = {
            'u': AIRSONIC_USER,
            'v': AIRSONIC_API_VERSION,
            'c': 'csv_checker',
            'f': 'json',
            'query': album,  # Buscar por nombre de álbum
            'albumCount': 50,  # Número de álbumes a buscar
        }

        # Añadir autenticación según el método configurado
        if AIRSONIC_AUTH_METHOD == 'token':
            # Autenticación con token MD5
            salt = 'airsonic'
            token = generate_token(AIRSONIC_PASS, salt)
            params['t'] = token
            params['s'] = salt
        else:
            # Autenticación con contraseña en texto plano (por defecto)
            params['p'] = AIRSONIC_PASS

        # Hacer la petición a la API
        url = urljoin(AIRSONIC_URL, '/rest/search3')
        response = requests.get(url, params=params, timeout=10)

        if response.status_code != 200:
            print(f"Error HTTP {response.status_code} al buscar: {artist} - {album}", file=sys.stderr)
            return False

        data = response.json()

        # Verificar si la respuesta es exitosa
        if data.get('subsonic-response', {}).get('status') != 'ok':
            print(f"Error en API Airsonic: {data}", file=sys.stderr)
            return False

        # Buscar en los resultados
        search_result = data.get('subsonic-response', {}).get('searchResult3', {})
        albums = search_result.get('album', [])

        if not albums:
            return False

        # Normalizar nombres para comparación (sin acentos, minúsculas, sin espacios extra)
        artist_normalized = normalize_text(artist)
        album_normalized = normalize_text(album)

        # Buscar coincidencia exacta o parcial
        for found_album in albums:
            found_artist = found_album.get('artist', '')
            found_name = found_album.get('name', '')

            # Normalizar los resultados de Airsonic también
            found_artist_normalized = normalize_text(found_artist)
            found_name_normalized = normalize_text(found_name)

            # Coincidencia exacta (ambos normalizados)
            if found_artist_normalized == artist_normalized and found_name_normalized == album_normalized:
                return True

            # Coincidencia parcial (el artista coincide y el álbum está contenido)
            if found_artist_normalized == artist_normalized and album_normalized in found_name_normalized:
                return True

        return False

    except requests.exceptions.RequestException as e:
        print(f"Error de conexión al buscar {artist} - {album}: {e}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"Error inesperado al buscar {artist} - {album}: {e}", file=sys.stderr)
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Verifica qué álbumes del CSV ya existen en Airsonic'
    )
    parser.add_argument(
        'input_csv',
        help='Archivo CSV de entrada (formato: artista,álbum)'
    )
    parser.add_argument(
        '--mode',
        choices=['annotate', 'missing', 'found', 'split', 'clean'],
        default='annotate',
        help='Modo de salida: annotate (añadir columna), missing (solo no encontrados), found (solo encontrados), split (dos archivos), clean (editar CSV eliminando encontrados)'
    )
    parser.add_argument(
        '-o', '--output',
        default='releases_checked.csv',
        help='Archivo CSV de salida (default: releases_checked.csv). En modo clean, si no se especifica, sobrescribe el archivo de entrada'
    )
    parser.add_argument(
        '--backup',
        action='store_true',
        help='Crear copia de seguridad antes de editar (solo en modo clean)'
    )

    args = parser.parse_args()

    # Verificar que el archivo de entrada existe
    if not os.path.exists(args.input_csv):
        print(f"Error: No se encuentra el archivo {args.input_csv}", file=sys.stderr)
        sys.exit(1)

    # Leer el CSV
    releases = []
    print(f"Leyendo {args.input_csv}...", file=sys.stderr)
    with open(args.input_csv, 'r', newline='', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2:
                releases.append((row[0], row[1]))

    print(f"Encontrados {len(releases)} álbumes en el CSV", file=sys.stderr)
    print(f"Verificando en Airsonic ({AIRSONIC_URL})...\n", file=sys.stderr)

    # Verificar cada álbum en Airsonic
    results = []
    found_count = 0
    missing_count = 0

    for i, (artist, album) in enumerate(releases, 1):
        print(f"[{i}/{len(releases)}] Verificando: {artist} - {album}...", end=' ', file=sys.stderr)

        found = search_album_in_airsonic(artist, album)
        results.append((artist, album, found))

        if found:
            print("✅ ENCONTRADO", file=sys.stderr)
            found_count += 1
        else:
            print("❌ NO ENCONTRADO", file=sys.stderr)
            missing_count += 1

    # Generar salida según el modo
    if args.mode == 'annotate':
        # Añadir columna con estado
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for artist, album, found in results:
                writer.writerow([artist, album, 'Si' if found else 'No'])
        print(f"\n✓ CSV generado con columna de estado: {args.output}", file=sys.stderr)

    elif args.mode == 'missing':
        # Solo los que NO están en Airsonic
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for artist, album, found in results:
                if not found:
                    writer.writerow([artist, album])
        print(f"\n✓ CSV generado solo con álbumes faltantes: {args.output}", file=sys.stderr)

    elif args.mode == 'found':
        # Solo los que SÍ están en Airsonic
        with open(args.output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for artist, album, found in results:
                if found:
                    writer.writerow([artist, album])
        print(f"\n✓ CSV generado solo con álbumes encontrados: {args.output}", file=sys.stderr)

    elif args.mode == 'split':
        # Crear dos archivos separados
        base_name = os.path.splitext(args.output)[0]
        found_file = f"{base_name}_encontrados.csv"
        missing_file = f"{base_name}_faltantes.csv"

        with open(found_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for artist, album, found in results:
                if found:
                    writer.writerow([artist, album])

        with open(missing_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for artist, album, found in results:
                if not found:
                    writer.writerow([artist, album])

        print(f"\n✓ CSVs generados:", file=sys.stderr)
        print(f"  - Encontrados: {found_file}", file=sys.stderr)
        print(f"  - Faltantes: {missing_file}", file=sys.stderr)

    elif args.mode == 'clean':
        # Editar CSV eliminando álbumes encontrados
        # Determinar archivo de salida
        output_file = args.output if args.output != 'releases_checked.csv' else args.input_csv

        # Crear backup si se solicita
        if args.backup:
            backup_file = f"{args.input_csv}.backup"
            import shutil
            shutil.copy2(args.input_csv, backup_file)
            print(f"\n✓ Backup creado: {backup_file}", file=sys.stderr)

        # Escribir solo los álbumes NO encontrados
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            for artist, album, found in results:
                if not found:
                    writer.writerow([artist, album])

        print(f"\n✓ CSV editado (eliminados {found_count} álbumes que ya tienes): {output_file}", file=sys.stderr)
        if output_file == args.input_csv:
            print(f"  ⚠ El archivo original ha sido modificado", file=sys.stderr)

    # Mostrar resumen
    print(f"\n{'='*50}", file=sys.stderr)
    print(f"RESUMEN", file=sys.stderr)
    print(f"{'='*50}", file=sys.stderr)
    print(f"Total álbumes verificados: {len(releases)}", file=sys.stderr)
    print(f"Encontrados en Airsonic:   {found_count} ({found_count/len(releases)*100:.1f}%)", file=sys.stderr)
    print(f"No encontrados:            {missing_count} ({missing_count/len(releases)*100:.1f}%)", file=sys.stderr)


if __name__ == '__main__':
    main()
