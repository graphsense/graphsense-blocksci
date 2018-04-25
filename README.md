# BlockSci

## Docker container

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

## BlockSci Usage

To parse the binary Bitcoin data from directly from Leveldb
```
docker exec -ti blocksci blocksci_parser --output-directory /var/data/blocksci_data update disk --coin-directory /var/data/block_data
```

To use BlockSci interactively in iPython, execute
```
docker exec -ti blocksci ipython3

In [1]: import blocksci
In [2]: import blocksci.cluster_python
In [3]: import pandas as pd
In [4]: chain = blocksci.Blockchain("/var/data/blocksci_data/")
...
```

