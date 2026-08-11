"""Comprueba si un álbum ya está en Airsonic o en qBittorrent (redundante,
no hace falta mostrarlo/descargarlo de nuevo). Usado por buscar_nuevos.py,
top_semana.py y limpiar_ya_tengo.py -- una sola implementación para no
divergir entre los tres puntos de entrada."""

import os

from airsonic_clean_csv import search_album_in_airsonic
from qbittorrent_cleaner_csv import _album_in_torrent

QB_HOST = os.getenv("QB_HOST", "localhost")
QB_PORT = os.getenv("QB_PORT", "8080")
QB_USER = os.getenv("QB_USER", "admin")
QB_PASS = os.getenv("QB_PASS", "adminadmin")


def qb_torrent_names() -> list[str]:
    """Nombres de todos los torrents en qBittorrent, o [] si no está disponible."""
    try:
        from qbittorrentapi import Client
        client = Client(host=QB_HOST, port=QB_PORT, username=QB_USER, password=QB_PASS)
        client.auth_log_in()
        return [t.name for t in client.torrents_info()]
    except Exception as e:
        print(f"⚠️  qBittorrent no disponible, se omite la comprobación: {e}")
        return []


def ya_lo_tengo(artist: str, album: str, torrent_names: list[str]) -> bool:
    """True si el álbum ya está en Airsonic o en qBittorrent."""
    if search_album_in_airsonic(artist, album):
        return True
    return any(_album_in_torrent(artist, album, t) for t in torrent_names)
