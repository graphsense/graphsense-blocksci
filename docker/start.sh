docker stop blocksci
docker rm blocksci
docker run --restart=always -d --name blocksci \
        -v /var/data/graphsense/leveldb/btc:/var/data/block_data \
        -v /var/data/temp/blocksci:/var/data/blocksci_data \
	-it blocksci
docker ps -a
