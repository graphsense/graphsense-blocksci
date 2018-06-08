#!/bin/sh

if [ $# -ne 2 ]; then
    echo "Usage: $0 BLOCKCHAIN_DATA_DIR BLOCKSCI_DATA_DIR"
    exit 1
fi

if [ ! -d "$1" ]; then
    echo "Directory $1 does not exist"
    exit 1
fi

if [ ! -d "$2" ]; then
    echo "Directory $2 does not exist"
    exit 1
fi

docker stop blocksci
docker rm blocksci
docker run --restart=always -d --name blocksci \
    -v "$1":/var/data/block_data -v "$2":/var/data/blocksci_data \
    -it blocksci
docker ps -a
