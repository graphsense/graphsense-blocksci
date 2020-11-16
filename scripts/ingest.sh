#!/bin/sh
python3 -u /usr/local/bin/blocksci_export.py \
    -c /opt/graphsense/blocksci.cfg \
    -d $CASSANDRA_HOST \
    -k $RAW_KEYSPACE \
    --processes $PROCESSES \
    --continue \
    --previous_day
