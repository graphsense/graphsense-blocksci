#!/bin/sh

if [ $# -ne 4 ]; then
    echo "Usage: $0 CONTAINER_NAME BLOCKCHAIN_DATA_DIR BLOCKSCI_DATA_DIR"
    exit 1
fi

if [ ! -d "$2" ]; then
    echo "Directory $2 does not exist"
    exit 1
fi

if [ ! -d "$3" ]; then
    echo "Directory $3 does not exist"
    exit 1
fi

if [ ! -d "$4" ]; then
    echo "Directory $4 does not exist"
    exit 1
fi

docker stop "$1"
docker rm "$1"
docker run --restart=always -d --name "$1" \
    --network=graphsense-net \
    --cap-drop all \
    -v "$2":/var/data/block_data \
    -v "$3":/var/data/blocksci_data \
    -v "$4":/opt/scripts \
    -it blocksci
docker ps -a
