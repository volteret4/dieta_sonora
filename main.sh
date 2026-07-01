DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/"
VENV_PYTHON="$HOME/Scripts/python_venv/bin/python3"
if [ -x "$VENV_PYTHON" ]; then
    PYTHON="$VENV_PYTHON"
else
    PYTHON="python3"
fi
CSV_FILE="albums.csv"

cd "$DIR" || exit 1

# Guardar estado antes
LINEAS_ANTES=$(wc -l < "$CSV_FILE")
CHECKSUM_ANTES=$(md5sum "$CSV_FILE" | awk '{print $1}')

# comprueba calendario hoy
"$PYTHON" revisor_calendario.py --since 21

# añade top 10 orpheus
"$PYTHON" top_10.py
# elimina los albumes que tienes en airsonic
"$PYTHON" airsonic_clean_csv.py --mode clean albums.csv

# elimina los albumes en qbitorrent
$PYTHON qbittorrent_cleaner_csv.py --clean

# Calcular estado después
LINEAS_DESPUES=$(wc -l < "$CSV_FILE")
CHECKSUM_DESPUES=$(md5sum "$CSV_FILE" | awk '{print $1}')


# Comparar
if [ "$CHECKSUM_ANTES" != "$CHECKSUM_DESPUES" ]; then
    ELIMINADOS=$((LINEAS_ANTES - LINEAS_DESPUES))

    echo "=========================================="
    echo "CSV MODIFICADO"
    echo "=========================================="
    echo "Antes:      $LINEAS_ANTES álbumes"
    echo "Después:    $LINEAS_DESPUES álbumes"
    echo "Eliminados: $ELIMINADOS álbumes"
    echo ""

    # busca los que no tienes en orpheus
    echo "Buscando en Orpheus"
    $PYTHON buscar_nuevos.py

    # crear html
    echo "Creando HTML"
    $PYTHON html_generator.py

    # renombrar html
    mv resumen_flacs.html index.html
    
else
    echo "CSV sin cambios - no hay álbumes que eliminar"
fi

# Copiar archivos HTML generados a SWAG || Comentado porque el contenedor de docker monta este directorio
#cp "$HOME"/Scripts/Musica/orpheus-api/index.html "$HOME"/contenedores/herramientas/swag/config/www/musica/
#cp "$HOME"/Scripts/Musica/orpheus-api/resumen_flacs.html "$HOME"/contenedores/herramientas/swag/config/www/musica/discos_nuevos.html
