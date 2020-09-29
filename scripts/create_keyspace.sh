#!/bin/sh

if [ $# -ne 2 ]; then
    echo "Usage: $0 CASSANDRA_NODE KEYSPACE"
    exit 1
fi

m4 -Dgraphsense=$2 schema.cql | cqlsh $1 --cqlversion="3.4.4"
