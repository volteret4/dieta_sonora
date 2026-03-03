#!/usr/bin/env bash
#
# Script Name: top10.sh 
# Description: _ _ _
# Author: volteret4
# Repository: https://github.com/volteret4/
# License: 
# Notes:
#
#

carpeta=$(dirname $(readlink "$0"))
source "${carpeta}/.env"

# nombre del archivo donde se guardará el contenido del JSON
json_top10="/home/dietpi/nodeRED/orpheus/top10.json"

#ORPHEUS_APIKEY=""
#JOIN_APIKEY=""
#POCOX3=""



curl -s 'https://orpheus.network/ajax.php?action=top10' -H "Authorization: ${ORPHEUS_APIKEY}" | jq . > $json_top10

group_ids=$(cat ${json_top10} | jq -r '.response[].results[].groupId')

for group_ids in $group_ids; do

    # Realiza la consulta a la API de Gazelle
    response=$(curl -s "https://orpheus.network/ajax.php?action=torrentgroup&id=$groupId -H "Authorization: ${ORPHEUS_APIKEY}" | jq .")

    # Extrae la URL de la imagen de "wikiImage" y el nombre de "name"
    url=$(echo "$response" | jq -r '.response.group.wikiImage')
    name=$(echo "$response" | jq -r '.response.group.name')

    # Descarga la imagen y guárdala con el nombre "name"
    wget -O "$name.jpg" "$url"
done