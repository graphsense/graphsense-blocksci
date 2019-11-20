#!/bin/sh

if [ $# -ne 3 ]; then
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

docker stop "$1"
docker rm "$1"
docker run --restart=always -d --name "$1" \
    --cap-drop all \
    -v "$2":/var/data/block_data -v "$3":/var/data/blocksci_data \
    -it blocksci
docker ps -a
