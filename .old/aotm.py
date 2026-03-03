#!/usr/bin/env python
#
# Script Name: aotm.py 
# Description: _ _ _
# Author: volteret4
# Repository: https://github.com/volteret4/
# License: 
# Notes:
#
#

carpeta=$(dirname $(readlink "$0"))
source "${carpeta}/.env"

# Variables para la API Gazelle
api_url="https://orpheus.network/ajax.php"
# ORPHEUS_API_KEY

# Variables para el foro que deseas monitorear
forum_id="51"  # ID del foro en Gazelle
search_string="album of the month"

# Realiza la solicitud a la API Gazelle para obtener los nuevos hilos en el foro especificado
response=$()

