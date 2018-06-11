#!/bin/sh

if [ $# -ne 2 ]; then
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

docker exec -ti "$1" blocksci_parser --output-directory "$3" update --max-block -6 disk --coin-directory "$2"
