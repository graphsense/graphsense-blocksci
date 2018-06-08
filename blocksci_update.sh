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

docker exec -ti blocksci blocksci_parser --output-directory "$2" update --max-block -6 disk --coin-directory "$1"
