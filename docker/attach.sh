#!/bin/sh

if [ $# -ne 3 ]; then
    echo "Usage: $0 CONTAINER_NAME"
    exit 1
fi

docker exec -ti "$1" /bin/bash
