#!/bin/sh
echo "Creating Cassandra keyspace"
python3 /usr/local/bin/create_keyspace.py -d ${CASSANDRA_HOST} -k ${RAW_KEYSPACE} -s /opt/graphsense/schema.cql
exec "$@"
