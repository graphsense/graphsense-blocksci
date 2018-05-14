
# Export of BlockSci data to Apache Cassandra 

## BlockSci Docker container

Build docker image

```
docker build -t blocksci .
```
or `./docker/build.sh`

Start docker container
```
./docker/start.sh
```

Attach docker container
```
docker exec -ti blocksci /bin/bash
```
or `./docker/attach.sh`

## BlockSci export

To parse the binary Bitcoin data from directly from Leveldb
```
docker exec -ti blocksci blocksci_parser --output-directory /var/data/blocksci_data update disk --coin-directory /var/data/block_data
```

To export BlockSci blockchain data to Apache Cassandra, create a keyspace

```
cqlsh $CASSANDRA_HOST -f schema.cql
```

and use the `blocksci_export.py` script:

```
blocksci_export.py -h
usage: blocksci_export.py [-h] [-c CASSANDRA] -k KEYSPACE -b BLOCKSCI_DATA
                          [-p NUM_PROC]

optional arguments:
  -h, --help            show this help message and exit
  -c CASSANDRA, --cassandra CASSANDRA
                        cassandra nodes (comma separated list of nodes)
  -k KEYSPACE, --keyspace KEYSPACE
                        keyspace to import data to
  -b BLOCKSCI_DATA, --blocksci_data BLOCKSCI_DATA
                        source directory for raw JSON block dumps
  -p NUM_PROC, --processes NUM_PROC
                        number of processes
```
