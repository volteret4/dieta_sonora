
Estructura de la tabla: artists
(0, 'artist_id', 'INTEGER', 0, None, 1)
(1, 'name', 'TEXT', 1, None, 0)
(2, 'name_normalized', 'TEXT', 1, None, 0)
(3, 'mbid', 'TEXT', 0, None, 0)
(4, 'genres_fetched', 'INTEGER', 0, '0', 0)

Estructura de la tabla: sqlite_sequence
(0, 'name', '', 0, None, 0)
(1, 'seq', '', 0, None, 0)

Estructura de la tabla: albums
(0, 'album_id', 'INTEGER', 0, None, 1)
(1, 'artist_id', 'INTEGER', 1, None, 0)
(2, 'name', 'TEXT', 1, None, 0)
(3, 'name_normalized', 'TEXT', 1, None, 0)
(4, 'mbid', 'TEXT', 0, None, 0)

Estructura de la tabla: genres
(0, 'genre_id', 'INTEGER', 0, None, 1)
(1, 'name', 'TEXT', 1, None, 0)
(2, 'name_normalized', 'TEXT', 1, None, 0)

Estructura de la tabla: artist_genres
(0, 'artist_id', 'INTEGER', 1, None, 1)
(1, 'genre_id', 'INTEGER', 1, None, 2)

Estructura de la tabla: scrobbles
(0, 'scrobble_id', 'INTEGER', 0, None, 1)
(1, 'artist_id', 'INTEGER', 1, None, 0)
(2, 'album_id', 'INTEGER', 0, None, 0)
(3, 'track', 'TEXT', 1, None, 0)
(4, 'ts', 'INTEGER', 1, None, 0)
(5, 'ts_iso', 'TEXT', 1, None, 0)
(6, 'source', 'TEXT', 1, "'lastfm'", 0)
(7, 'track_normalized', 'TEXT', 1, "''", 0)

Estructura de la tabla: sync_state
(0, 'key', 'TEXT', 0, None, 1)
(1, 'value', 'TEXT', 0, None, 0)
