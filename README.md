# A dockerized component to synchronize BlockSci data to Apache Cassandra

## Quick docker setup

### Prerequisites
Make sure the latest versions of Docker and docker-compose are installed. https://docs.docker.com/compose/install/

This docker composition consists of two services:
 - Parser, which reads Bitcoin client's data directory and puts it into a more useful format.
 - Exporter, which copies data read by parser to cassandra's raw keyspace.

Parser assumes that:
 - A [bitcoin client](https://bitcoin.org/en/download) has been running for a while and has fetched some blocks. A `BLOCK_DATADIR` variable points to data directory of Bitcoin client.
 - `BLOCKSCI_DATADIR` variable points to a directory with enough free space (around the size of blockchain).
 
 Exporter assumes that:
  - A parser has completed parsing block files;
  - There is a cassandra instance running.

**It is possible to set up all required services using a single docker-compose evironment. For that, check out the `graphsense-setup` project.** Alternatively, you can set up each required service manually, in which case, keep on reading.

### Configuration
Create a new configuration by copying the `env.example` file to `.env`.
Modify the configuration match your environment, or keep everything intact.

- `BLOCK_DATADIR` must point to data directory of a Bitcoin client.
- `FROM_BLOCK` and `TO_BLOCK` must specify block range to process. Set the latter to a negative number to count backwards from the latest block, or to `-1` to fetch up until the last block.
- Change `PROCESSES` variable to the number of cores you want to give to exporter.


Apply the configuation by adding this line to `docker-compose.yml`:
```yaml
services:
    transform:
        ...
        env_file: .env
        ...
```


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
