
# Export of BlockSci data to Apache Cassandra 

## BlockSci Docker container

Build docker image

```
docker build -t blocksci .
```
or `./docker/build.sh`

Start docker container
```
./docker/start.sh CONTAINER_NAME BLOCKCHAIN_DATA_DIR BLOCKCHAIN_DATA_DIR
```

`CONTAINER_NAME` specifies the name of the docker container;
`BLOCKCHAIN_DATA_DIR` and `BLOCKSCI_DATA_DIR` are the locations of the
data directories.

Attach docker container
```
docker exec -ti blocksci /bin/bash
```
or `./docker/attach.sh`

## BlockSci export

To parse the binary Bitcoin data from directly from Leveldb, use
```
docker exec -ti blocksci blocksci_parser --output-directory BLOCKSCI_DATA_DIR update --max-block -6 disk --coin-directory BLOCKCHAIN_DATA_DIR
```

To export BlockSci blockchain data to Apache Cassandra, create a keyspace

```
cqlsh $CASSANDRA_HOST -f schema.cql
```

and use the `blocksci_export.py` script:

```
blocksci_export.py -h
usage: blocksci_export.py [-h] -b BLOCKSCI_DATA [-c CASSANDRA_HOSTS] -k
                          KEYSPACE [-p NUM_PROC] [-s START_INDEX]
                          [-e END_INDEX]

Export dumped BlockSci data to Apache Cassandra

optional arguments:
  -h, --help            show this help message and exit
  -b BLOCKSCI_DATA, --blocksci_data BLOCKSCI_DATA
                        directory containing the parsed BlockSci data
  -c CASSANDRA_HOSTS, --cassandra CASSANDRA_HOSTS
                        Cassandra nodes (comma separated list of nodes;
                        default "localhost")
  -k KEYSPACE, --keyspace KEYSPACE
                        Cassandra keyspace
  -p NUM_PROC, --processes NUM_PROC
                        number of processes (default 1)
  -s START_INDEX, --start_index START_INDEX
                        start index of the blocks to export (default 0)
  -e END_INDEX, --end_index END_INDEX
                        only blocks with height smaller than this value are
                        included; a negative index counts back from the end
                        (default -1)

GraphSense - http://graphsense.info
```
