PYTHON="$HOME/Scripts/python_venv/bin/python"
DIR="$HOME/Scripts/Musica/orpheus-api/"
CSV_FILE="albums.csv"

cd "$DIR" || exit 1

# Guardar estado antes
LINEAS_ANTES=$(wc -l < "$CSV_FILE")
CHECKSUM_ANTES=$(md5sum "$CSV_FILE" | awk '{print $1}')

# comprueba calendario hoy
$PYTHON revisor_calendario.py

# elimina los albumes que tienes
$PYTHON airsonic_checker.py --mode clean albums.csv

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
else
    echo "CSV sin cambios - no hay álbumes que eliminar"
fi

# Copiar archivos HTML generados a SWAG
cp "$HOME"/Scripts/Musica/orpheus-api/index.html "$HOME"/contenedores/herramientas/swag/config/www/musica/
cp "$HOME"/Scripts/Musica/orpheus-api/resumen_flacs.html "$HOME"/contenedores/herramientas/swag/config/www/musica/discos_nuevos.html
