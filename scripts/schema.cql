CREATE KEYSPACE IF NOT EXISTS graphsense
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE graphsense;

CREATE TABLE block (
    height int PRIMARY KEY,
    block_hash blob,
    timestamp int,
    no_transactions int
);

CREATE TYPE tx_input_output (
    address list<text>,
    value bigint,
    address_type smallint
);

CREATE TABLE transaction (
    tx_prefix text,
    tx_hash blob,
    tx_index bigint,
    height int,
    timestamp int,
    coinbase boolean,
    total_input bigint,
    total_output bigint,
    inputs list<FROZEN<tx_input_output>>,
    outputs list<FROZEN<tx_input_output>>,
    coinjoin boolean,
    PRIMARY KEY (tx_prefix, tx_hash)
);

CREATE TYPE tx_summary (
    tx_hash blob,
    no_inputs int,
    no_outputs int,
    total_input bigint,
    total_output bigint
);

CREATE TABLE block_transactions (
    height int PRIMARY KEY,
    txs list<FROZEN<tx_summary>>
);

CREATE TABLE exchange_rates (
    date text PRIMARY KEY,
    eur float,
    usd float
);

CREATE TABLE summary_statistics (
    id text PRIMARY KEY,
    no_blocks int,
    no_txs bigint,
    timestamp int
);
