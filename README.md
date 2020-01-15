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

Start docker container

```
./docker/start.sh CONTAINER_NAME BLOCKCHAIN_DATA_DIR BLOCKSCI_DATA_DIR
```

`CONTAINER_NAME` specifies the name of the docker container;
`BLOCKCHAIN_DATA_DIR` and `BLOCKSCI_DATA_DIR` are the locations of the
data directories on the host system. `BLOCKCHAIN_DATA_DIR` is mapped to
`/var/data/block_data`, and `BLOCKSCI_DATA_DIR` corresponds to
`/var/data/blocksci_data` inside the docker container.

Attach docker container

```
docker exec -ti blocksci_btc /bin/bash
```

or `./docker/attach.sh blocksci_btc`

## BlockSci export

Create a BlockSci config file, e.g., for Bitcoin using the disk mode parser

```
blocksci_parser /var/data/blocksci_data/btc.cfg generate-config bitcoin /var/data/blocksci_data --max-block '-6' --disk /var/data/block_data
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
usage: blocksci_export.py [-h] -c BLOCKSCI_CONFIG [-d DB_NODE [DB_NODE ...]]
                          -k KEYSPACE [--processes NUM_PROC]
                          [--chunks NUM_CHUNKS] [-p]
                          [--start_index START_INDEX] [--end_index END_INDEX]
                          [--blocks] [--block_tx] [--tx] [--statistics]

Export dumped BlockSci data to Apache Cassandra

optional arguments:
  -h, --help            show this help message and exit
  -c BLOCKSCI_CONFIG, --config BLOCKSCI_CONFIG
                        BlockSci configuration file
  -d DB_NODE [DB_NODE ...], --db_nodes DB_NODE [DB_NODE ...]
                        list of Cassandra nodes; default "localhost")
  -k KEYSPACE, --keyspace KEYSPACE
                        Cassandra keyspace
  --processes NUM_PROC  number of processes (default 1)
  --chunks NUM_CHUNKS   number of chunks to split tx/block range (default
                        `NUM_PROC`)
  -p, --previous_day    only ingest blocks up to the previous day, since
                        currency exchange rates might not be available for the
                        current day.
  --start_index START_INDEX
                        start index of the blocks to export (default 0)
  --end_index END_INDEX
                        only blocks with height smaller than this value are
                        included; a negative index counts back from the end
                        (default -1)
  --blocks              ingest only into the blocks table
  --block_tx            ingest only into the block_transactions table
  --tx                  ingest only into the transactions table
  --statistics          ingest only into the summary statistics table

GraphSense - http://graphsense.info
```

## Exchange rates

For Bitcoin we use the [CoinDesk API][coindesk] to obtain exchange rates, see
`scripts/ingest_rates_coindesk.py`:

```
python3 scripts/ingest_rates_coindesk.py -h
usage: ingest_rates_coindesk.py [-h] [-d DB_NODE [DB_NODE ...]] [-f] -k
                                KEYSPACE [-t TABLE] [--start_date START]
                                [--end_date END]

Ingest exchange rates into Cassandra

optional arguments:
  -h, --help            show this help message and exit
  -d DB_NODE [DB_NODE ...], --db_nodes DB_NODE [DB_NODE ...]
                        list of Cassandra nodes; default "localhost"
  -f, --force           do not fetch most recent entries from Cassandra and
                        overwrite existing records
  -k KEYSPACE, --keyspace KEYSPACE
                        Cassandra keyspace
  -t TABLE, --table TABLE
                        name of the target exchange rate table
  --start_date START    start date for fetching exchange rates
  --end_date END        end date for fetching exchange rates

GraphSense - http://graphsense.info

```

For all other currencies the exchange rates are obtained through
[CoinMarketCap][coinmarketcap], see `scripts/ingest_rates_coinmarketcap.py`:

```
python3 scripts/ingest_rates_coinmarketcap.py -h
usage: ingest_rates_coinmarketcap.py [-h] [-d DB_NODE [DB_NODE ...]] [-f] -k
                                     KEYSPACE [-t TABLE] [--start_date START]
                                     [--end_date END] -c CRYPTOCURRENCY

Ingest exchange rates into Cassandra

optional arguments:
  -h, --help            show this help message and exit
  -d DB_NODE [DB_NODE ...], --db_nodes DB_NODE [DB_NODE ...]
                        list of Cassandra nodes; default "localhost"
  -f, --force           do not fetch most recent entries from Cassandra and
                        overwrite existing records
  -k KEYSPACE, --keyspace KEYSPACE
                        Cassandra keyspace
  -t TABLE, --table TABLE
                        name of the target exchange rate table
  --start_date START    start date for fetching exchange rates
  --end_date END        end date for fetching exchange rates
  -c CRYPTOCURRENCY, --cryptocurrency CRYPTOCURRENCY
                        target cryptocurrency

GraphSense - http://graphsense.info
```

[apache-cassandra]: http://cassandra.apache.org/download
[coindesk]: https://www.coindesk.com/api
[coinmarketcap]: https://coinmarketcap.com
