# A dockerized component to synchronize BlockSci data to Apache Cassandra

## Prerequisites

### Apache Cassandra

Download and install [Apache Cassandra][apache-cassandra] >= 3.11
in `$CASSANDRA_HOME`.

Start Cassandra (in the foreground for development purposes):

    $CASSANDRA_HOME/bin/cassandra -f

Connect to Cassandra via CQL

    $CASSANDRA_HOME/bin/cqlsh

and test if it is running

    cqlsh> SELECT cluster_name, listen_address FROM system.local;

    cluster_name | listen_address
    --------------+----------------
    Test Cluster |      127.0.0.1

    (1 rows)

## BlockSci Docker container

Build docker image

```
docker build -t blocksci .
```

or `./docker/build.sh`

Create an user-defined bridge network

```
docker network create graphsense-net
```

Start docker container

```
./docker/start.sh CONTAINER_NAME BLOCKCHAIN_DATA_DIR BLOCKSCI_DATA_DIR SCRIPT_DIR
```

`CONTAINER_NAME` specifies the name of the docker container;
`BLOCKCHAIN_DATA_DIR` and `BLOCKSCI_DATA_DIR` are the locations of the
data directories on the host system, and `SCRIPT_DIR` the location of
additional scripts or other files. They arguments are mapped to the following
locations inside the docker container:

- `BLOCKCHAIN_DATA_DIR`: `/var/data/block_data`
- `BLOCKSCI_DATA_DIR`: `/var/data/blocksci_data`
- `SCRIPT_DIR`: `/opt/scripts`

Attach docker container

```
docker exec -ti blocksci_btc /bin/bash
```

or `./docker/attach.sh blocksci_btc`

## BlockSci export

Create a BlockSci config file, e.g., for Bitcoin using the disk mode parser

```
blocksci_parser /var/data/blocksci_data/btc.cfg generate-config bitcoin \
                /var/data/blocksci_data --max-block '-6' \
                --disk /var/data/block_data
```

To run the BlockSci parser, use

```
blocksci_parser /var/data/blocksci_data/btc.cfg update
```

To export BlockSci blockchain data to Apache Cassandra, create a keyspace

```
cqlsh $CASSANDRA_HOST -f scripts/schema.cql
```

and use the `blocksci_export.py` script:

```
python3 blocksci_export.py -h
usage: blocksci_export.py [-h] [--bip30-fix] -c BLOCKSCI_CONFIG [--concurrency CONCURRENCY]
                          [--continue] --db-keyspace KEYSPACE [--db-nodes DB_NODE [DB_NODE ...]]
                          [--db-port DB_PORT] [-i] [--processes NUM_PROC] [--chunks NUM_CHUNKS] [-p]
                          [--start-index START_INDEX] [--end-index END_INDEX]
                          [-t [TABLE [TABLE ...]]]

Export dumped BlockSci data to Apache Cassandra

optional arguments:
  -h, --help            show this help message and exit
  --bip30-fix           ensure for duplicated tx hashes, that the most recent hash is ingested as
                        specified in BIP30
  -c BLOCKSCI_CONFIG, --config BLOCKSCI_CONFIG
                        BlockSci configuration file
  --concurrency CONCURRENCY
                        Cassandra concurrency parameter (default 100)
  --continue            continue ingest from last block/tx id
  --db-keyspace KEYSPACE
                        Cassandra keyspace
  --db-nodes DB_NODE [DB_NODE ...]
                        list of Cassandra nodes; default "localhost")
  --db-port DB_PORT     Cassandra CQL native transport port; default 9042
  -i, --info            display block information and exit
  --processes NUM_PROC  number of processes (default 1)
  --chunks NUM_CHUNKS   number of chunks to split tx/block range (default `NUM_PROC`)
  -p, --previous-day    only ingest blocks up to the previous day, since currency exchange rates
                        might not be available for the current day
  --start-index START_INDEX
                        start index of the blocks to export (default 0)
  --end-index END_INDEX
                        only blocks with height smaller than or equal to this value are included; a
                        negative index counts back from the end (default -1)
  -t [TABLE [TABLE ...]], --tables [TABLE [TABLE ...]]
                        list of tables to ingest, possible values: "block" (block table), "block_tx"
                        (block transactions table), "tx" (transactions table), "stats" (summary
                        statistics table); ingests all tables if not specified

GraphSense - http://graphsense.info
```

[apache-cassandra]: http://cassandra.apache.org/download
[graphsense-setup]: https://github.com/graphsense/graphsense-setup
[coindesk]: https://www.coindesk.com/api
[coinmarketcap]: https://coinmarketcap.com
[graphsense-cli]: https://github.com/graphsense/graphsense-lib#exchange-rates
