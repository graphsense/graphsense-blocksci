#!/bin/sh

if [ $# -ne 1 ]; then
    echo "Usage: $0 CONTAINER_NAME"
    exit 1
fi

docker exec -ti "$1" blocksci_parser --output-directory /var/data/blocksci_data update --max-block -6 disk --coin-directory /var/data/block_data
