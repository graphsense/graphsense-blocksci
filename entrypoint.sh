#!/bin/sh
blocksci_parser $CONFIG_FILE generate-config bitcoin /var/data/blocksci_data --max-block '-6' --disk /var/data/block_data
echo "Config created!"
blocksci_parser $CONFIG_FILE update
