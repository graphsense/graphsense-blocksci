#!/bin/sh
python3 -u /usr/local/bin/blocksci_export.py \
    --config /opt/graphsense/blocksci.cfg \
    --db_nodes "$CASSANDRA_HOST" \
    --keyspace "$RAW_KEYSPACE" \
    --processes "$PROCESSES" \
    --continue \
    --previous_day
