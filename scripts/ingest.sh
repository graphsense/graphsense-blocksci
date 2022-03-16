#!/bin/sh
python3 -u /usr/local/bin/blocksci_export.py \
    --config /opt/graphsense/blocksci.cfg \
    --db-nodes "$CASSANDRA_HOST" \
    --db-keyspace "$RAW_KEYSPACE" \
    --processes "$PROCESSES" \
    --continue \
    --previous-day
