#!/usr/bin/env bash
#
# Script Name: aotm.sh 
# Description: _ _ _
# Author: volteret4
# Repository: https://github.com/volteret4/
# License: 
# Notes:
#   .env content
#       ORPHEUS_APIKEY      JOIN_APIKEY     POCOX3
#

# nombre del archivo donde se guardará el contenido del JSON
json_top10="/home/dietpi/nodeRED/orpheus/top10.json"

#ORPHEUS_APIKEY="api key"
#JOIN_APIKEY="api key"
#POCOX3="device id"

curl -s 'https://orpheus.network/ajax.php?action=top10' -H "Authorization: ${api_key}" | jq . > ${json_top10}

group_ids=$(cat ${json_top10} | jq -r '.response[].results[].groupId')

for group_ids in $group_ids; do

    # Realiza la consulta a la API de Gazelle
    response=$(curl -s "https://orpheus.network/ajax.php?action=torrentgroup&id=$groupId -H "Authorization: ${api_key}" | jq .")

    # Extrae la URL de la imagen de "wikiImage" y el nombre de "name"
    url=$(echo "$response" | jq -r '.response.group.wikiImage')
    name=$(echo "$response" | jq -r '.response.group.name')

    # Descarga la imagen y guárdala con el nombre "name"
    wget -O "$name.jpg" "$url"
done



for group in $(cat top10.json | jq -r '.response[].results[].groupId'); do
    url=$(curl -s "https://orpheus.network/ajax.php?action=torrentgroup&id=" | grep -o 'https://example.com/static/common/images/.*jpg')
    if [ -n "$url" ]; then
        curl -s "$url" > "$group.jpg"
    fi
done





# función para comprobar si el JSON ha cambiado
function check_json_changes() {
    # obtener el contenido actual del JSON
    json=$(curl -s "https://orpheus.network/ajax.php?action=forum&type=viewforum&forumid=51" -H "Authorization: ${api_key}" | jq .)

    # si el archivo no existe, crearlo y guardar el contenido del JSON
    if [ ! -f "$JSON_FILE" ]; then
        echo "$json" > "$JSON_FILE"
    fi

    # comparar el contenido del archivo con el contenido actual del JSON
    if ! cmp -s "$JSON_FILE" <(echo "$json"); then
        # si el contenido ha cambiado, guardar el nuevo contenido en el archivo
        echo "$json" > "$JSON_FILE"

        # obtener el título del último tema y mostrar una notificación
        title=$(echo "$json" | jq '.[0].title')
        echo "El JSON ha cambiado. Nuevo título: $title"
        curl "https://joinjoaomgcd.appspot.com/_ah/api/messaging/v1/sendPush?apikey=${JOIN_APIKEY}&deviceId=${POCOX3}&text=${title}"
        else
            echo "same shit"

    fi
}

# llamar a la función para comprobar si el JSON ha cambiado
check_json_changes