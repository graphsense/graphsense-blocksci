#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from abc import ABC
from argparse import ArgumentParser
from collections import deque
from datetime import datetime as dt
from functools import wraps
from itertools import islice
from multiprocessing import Pool, Value
import time

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import SimpleStatement
import numpy as np
import blocksci


# dict(zip(blocksci.address_type.types,
#      range(1, len(blocksci.address_type.types) + 1)))
address_type = {
    'address_type.nonstandard': 1,
    'address_type.pubkey': 2,
    'address_type.pubkeyhash': 3,
    'address_type.multisig_pubkey': 4,
    'address_type.scripthash': 5,
    'address_type.multisig': 6,
    'address_type.nulldata': 7,
    'address_type.witness_pubkeyhash': 8,
    'address_type.witness_scripthash': 9,
    'address_type.witness_unknown': 10
}

TX_HASH_PREFIX_LENGTH = 5
TX_BUCKET_SIZE = 25_000
BLOCK_BUCKET_SIZE = 100


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        t1 = dt.now()
        result = f(*args, **kw)
        t2 = dt.now()
        print('\n... %s\n' % str(t2 - t1))
        return result
    return wrap


def query_most_recent_block(cluster, keyspace):
    '''Fetch most recent entry from blocks table, else return None.'''

    session = cluster.connect(keyspace)
    cql_str = "SELECT block_id_group FROM block PER PARTITION LIMIT 1"
    simple_stmt = SimpleStatement(cql_str, fetch_size=None)
    result = session.execute(simple_stmt)
    groups = [row.block_id_group for row in result.current_rows]

    if len(groups) == 0:
        return None

    max_block_group = max(groups)
    # query relies on CLUSTERING ORDER BY (block_id DESC)
    result = session.execute(f'SELECT block_id FROM {keyspace}.block '
                             f'WHERE block_id_group={max_block_group} LIMIT 1')
    latest_block = result.current_rows[0].block_id

    return latest_block


class QueryManager(ABC):

    counter = Value('d', 0)

    def __init__(self, cluster, keyspace, chain, cql_str,
                 num_proc=1, num_chunks=None, concurrency=100):
        if not num_chunks:
            num_chunks = num_proc
        self.num_proc = num_proc
        self.num_chunks = num_chunks
        init_args = (cluster, chain, keyspace, cql_str, concurrency)
        self.pool = Pool(processes=num_proc,
                         initializer=self._setup,
                         initargs=init_args)

    @classmethod
    def _setup(cls, cluster, chain, keyspace, cql_str, concurrency):
        cls.chain = chain
        cls.concurrency = concurrency
        cls.session = cluster.connect()
        cls.session.default_timeout = 60
        cls.session.set_keyspace(keyspace)
        cls.prepared_stmt = cls.session.prepare(cql_str)

    def close_pool(self):
        self.pool.close()
        self.pool.join()

    @timing
    def execute(self, fun, params):
        self.pool.map(fun, chunk(params, self.num_chunks))

    @classmethod
    def insert(cls, params):
        pass


class TxQueryManager(QueryManager):

    counter = Value('d', 0)

    @classmethod
    def insert(cls, params):

        idx_start, idx_end = params

        param_list = []

        for index in range(idx_start, idx_end, cls.concurrency):

            curr_batch_size = min(cls.concurrency, idx_end - index)
            for i in range(0, curr_batch_size):
                tx = blocksci.Tx(index + i, cls.chain)
                param_list.append(tx_summary(tx))

            results = execute_concurrent_with_args(
                session=cls.session,
                statement=cls.prepared_stmt,
                parameters=param_list,
                concurrency=cls.concurrency)
            for (i, (success, _)) in enumerate(results):
                if not success:
                    while True:
                        try:
                            tx = blocksci.Tx(index + i, cls.chain)
                            cls.session.execute(cls.prepared_stmt,
                                                tx_summary(tx))
                        except Exception as e:
                            print(e)
                            continue
                        break

            param_list = []

            with cls.counter.get_lock():
                cls.counter.value += curr_batch_size
            if (cls.counter.value % 1e4) == 0:
                print(f'#tx {cls.counter.value:,.0f}')


class TxLookupQueryManager(QueryManager):
    counter = Value('d', 0)

    @classmethod
    def insert_lookup_table(cls, params):
        idx_start, idx_end = params

        for index in range(idx_start, idx_end, cls.concurrency):
            param_list = deque()

            curr_batch_size = min(cls.concurrency, idx_end - index)
            for i in range(0, curr_batch_size):
                t_id = index + i
                tx = blocksci.Tx(t_id, cls.chain)
                param_list.append(tx_short_summary(tx.hash, t_id))

            results = execute_concurrent_with_args(
                session=cls.session,
                statement=cls.prepared_stmt,
                parameters=param_list,
                concurrency=cls.concurrency)

            for (i, (success, _)) in enumerate(results):
                if not success:
                    while True:
                        try:
                            t_id = index + i
                            tx = blocksci.Tx(t_id, cls.chain)
                            cls.session.execute(cls.prepared_stmt,
                                                tx_short_summary(tx.hash, t_id))
                        except Exception as e:
                            print(e)
                            continue
                        break

            with cls.counter.get_lock():
                cls.counter.value += curr_batch_size
            if (cls.counter.value % 1e4) == 0:
                print(f'#tx {cls.counter.value:,.0f}')


class BlockTxQueryManager(QueryManager):
    counter = Value('d', 0)

    @classmethod
    def insert(cls, params):

        idx_start, idx_end = params

        param_list = []

        for index in range(idx_start, idx_end, cls.concurrency):

            curr_batch_size = min(cls.concurrency, idx_end - index)
            for i in range(0, curr_batch_size):
                block = cls.chain[index + i]
                block_tx = [int(block.height // BLOCK_BUCKET_SIZE),
                            block.height,
                            [tx_stats(x) for x in block.txes]]
                param_list.append(block_tx)

            results = execute_concurrent_with_args(
                session=cls.session,
                statement=cls.prepared_stmt,
                parameters=param_list,
                concurrency=cls.concurrency)

            for (i, (success, _)) in enumerate(results):
                if not success:
                    while True:
                        try:
                            block = cls.chain[index + i]
                            block_tx = [block.height,
                                        [tx_stats(x) for x in block.txes]]
                            cls.session.execute(cls.prepared_stmt,
                                                block_tx)
                        except Exception as e:
                            print(e)
                            continue
                        break

            param_list = []

            with cls.counter.get_lock():
                cls.counter.value += curr_batch_size

            if (cls.counter.value % 1e4) == 0:
                print(f'#blocks {cls.counter.value:,.0f}')


@timing
def insert(cluster, keyspace, cql_stmt, generator, concurrency=100):
    session = cluster.connect(keyspace)
    session.default_timeout = 60
    prepared_stmt = session.prepare(cql_stmt)

    values = take(concurrency, generator)
    count = 0
    while values:

        results = execute_concurrent_with_args(
            session=session,
            statement=prepared_stmt,
            parameters=values,
            concurrency=concurrency)

        for (i, (success, _)) in enumerate(results):
            if not success:
                while True:
                    try:
                        session.execute(prepared_stmt, values[i])
                    except Exception as e:
                        print(e)
                        continue
                    break

        values = take(concurrency, generator)

        if (count % 1e4) == 0:
            print('#blocks {:,.0f}'.format(count))
        count += concurrency


def take(n, iterable):
    '''Return first n items of the iterable as a list

    >>> take(0, [1, 2])
    []

    >>> take(1, [1, 2])
    [1]

    >>> take(2, [1, 2])
    [1, 2]

    >>> take(3, [1, 2])
    [1, 2]
    '''

    return list(islice(iterable, n))


def chunk(val_range, k):
    '''Split the number range val_range=[n1, n2] into k evenly sized chunks

    >>> chunk([0, 1], 1)
    [(0, 1)]

    >>> chunk([0, 4], 4)
    [(0, 1), (1, 2), (2, 3), (3, 4)]

    >>> chunk([0, 5], 4)
    [(0, 2), (2, 3), (3, 4), (4, 5)]
    '''

    n1, n2 = val_range
    assert n2 > n1
    n = n2 - n1
    assert 0 < k <= n
    s, r = divmod(n, k)
    t = s + 1
    return ([(n1+p, n1+p+t) for p in range(0, r*t, t)] +
            [(n1+p, n1+p+s) for p in range(r*t, n, s)])


def addr_str(addr_obj):
    if addr_obj.type == blocksci.address_type.multisig:
        res = [x.address_string for x in addr_obj.addresses]
    elif addr_obj.type == blocksci.address_type.nonstandard:
        res = None
    elif addr_obj.type == blocksci.address_type.nulldata:
        res = None
    elif addr_obj.type == blocksci.address_type.witness_unknown:
        res = None
    else:
        res = [addr_obj.address_string]
    return res


def block_summary(block, bucket_size):
    return (int(block.height // bucket_size),
            block.height,
            bytearray.fromhex(str(block.hash)),
            block.timestamp,
            len(block))


def tx_stats(tx, bucket_size=TX_BUCKET_SIZE):
    return (tx.index,
            len(tx.inputs),
            len(tx.outputs),
            tx.input_value,
            tx.output_value)


def tx_io_summary(x):
    return [addr_str(x.address), x.value, address_type[repr(x.address_type)]]


def tx_summary(tx, bucket_size=TX_BUCKET_SIZE):
    tx_inputs = [tx_io_summary(x) for x in tx.inputs]
    tx_outputs = [tx_io_summary(x) for x in tx.outputs]
    return (int(tx.index // bucket_size),
            tx.index,
            bytearray.fromhex(str(tx.hash)),
            tx.block_height,
            int(tx.block_time.timestamp()),
            tx.is_coinbase,
            tx.input_value,
            tx.output_value,
            list(tx_inputs),
            list(tx_outputs),
            blocksci.heuristics.is_coinjoin(tx))


def tx_short_summary(tx_hash, t_id, prefix_length=TX_HASH_PREFIX_LENGTH):
    return (str(tx_hash)[:prefix_length],
            bytearray.fromhex(str(tx_hash)),
            t_id)


def insert_summary_stats(cluster, keyspace, last_block):
    total_blocks = last_block.height + 1
    total_txs = last_block.txes[-1].index + 1
    timestamp = last_block.timestamp

    session = cluster.connect(keyspace)
    cql_str = '''INSERT INTO summary_statistics
                 (id, timestamp, no_blocks, no_txs)
                 VALUES (%s, %s, %s, %s)'''
    session.execute(cql_str, (keyspace, timestamp, total_blocks, total_txs))


def create_parser():
    parser = ArgumentParser(description='Export dumped BlockSci data '
                                        'to Apache Cassandra',
                            epilog='GraphSense - http://graphsense.info')
    parser.add_argument("--bip30-fix", action='store_true',
                        help='ensure for duplicated tx hashes, that the most '
                             'recent hash is ingested as specified in BIP30')
    parser.add_argument('-c', '--config', dest='blocksci_config',
                        required=True,
                        help='BlockSci configuration file')
    parser.add_argument('--concurrency', dest='concurrency',
                        type=int, default=100,
                        help='Cassandra concurrency parameter (default 100)')
    parser.add_argument('--continue', action='store_true',
                        dest='continue_ingest',
                        help='continue ingest from last block/tx id')
    parser.add_argument('--db_nodes', dest='db_nodes', nargs='+',
                        default='localhost', metavar='DB_NODE',
                        help='list of Cassandra nodes; default "localhost")')
    parser.add_argument('--db_port', dest='db_port',
                        type=int, default=9042,
                        help='Cassandra CQL native transport port; '
                             'default 9042')
    parser.add_argument('-i', '--info', action='store_true',
                        help='display block information and exit')
    parser.add_argument('-k', '--keyspace', dest='keyspace', required=True,
                        help='Cassandra keyspace')
    parser.add_argument('--processes', dest='num_proc',
                        type=int, default=1,
                        help='number of processes (default 1)')
    parser.add_argument('--chunks', dest='num_chunks',
                        type=int,
                        help='number of chunks to split tx/block range '
                             '(default `NUM_PROC`)')
    parser.add_argument('-p', '--previous_day', dest='prev_day',
                        action='store_true',
                        help='only ingest blocks up to the previous day, '
                             'since currency exchange rates might not be '
                             'available for the current day')
    parser.add_argument('--start_index', dest='start_index',
                        type=int, default=0,
                        help='start index of the blocks to export '
                             '(default 0)')
    parser.add_argument('--end_index', dest='end_index',
                        type=int, default=-1,
                        help='only blocks with height smaller than or equal '
                             'to this value are included; a negative index '
                             'counts back from the end (default -1)')
    parser.add_argument('-t', '--tables', nargs='*', metavar='TABLE',
                        help='list of tables to ingest, possible values:'
                             '    "block" (block table), '
                             '    "block_tx" (block transactions table), '
                             '    "tx" (transactions table), '
                             '    "stats" (summary statistics table); '
                             'ingests all tables if not specified')
    return parser


def check_tables_arg(tables, table_list=['tx', 'block_tx', 'block', 'stats']):
    all_tables = tables is None
    table_list_intersect = table_list
    if not all_tables:
        set_diff = set(tables) - set(table_list)
        if len(tables) == 0:
            print('No tables specified in --tables/-t argument.')
            raise SystemExit(1)
        if set_diff:
            print('Unknown table(s) in --tables/-t argument:')
            for elem in set_diff:
                print(f'    {elem}')
            raise SystemExit(1)
        table_list_intersect = set(table_list).intersection(set(tables))

    print('Ingesting to tables:')
    for elem in table_list_intersect:
        print(f'    {elem}')

    return list(table_list_intersect)


def upsert_btc_duplicate_hashes(session, stmt):
    """Ensures for duplicated tx hashes that most recent transaction is
       ingested, since BIP30 dictates that it is the newest version of these
       transactions that is spendable.
       See https://bitcoin.stackexchange.com/a/88667/48795"""
    for tx, tid in [("e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468", 142841),
                    ("d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599", 142783)]:
        session.execute(stmt, tx_short_summary(tx, tid))


def main():
    parser = create_parser()
    args = parser.parse_args()

    chain = blocksci.Blockchain(args.blocksci_config)

    last_parsed_block = chain[-1]
    print('-' * 58)
    print('Last parsed block:   %10d (%s UTC)' %
          (last_parsed_block.height,
           dt.strftime(last_parsed_block.time, '%F %T')))

    cluster = Cluster(args.db_nodes, port=args.db_port)
    if args.continue_ingest:
        # get most recent block from database
        most_recent_block = query_most_recent_block(cluster, args.keyspace)
        if most_recent_block is not None and \
           most_recent_block > last_parsed_block.height:
            print('Error: inconsistent number of parsed and ingested blocks')
            raise SystemExit(1)
        if most_recent_block is None:
            next_block = 0
            print('Last ingested block:       None')
        else:
            next_block = most_recent_block + 1
            last_ingested_block = chain[most_recent_block]
            print('Last ingested block: %10d (%s UTC)' %
                  (last_ingested_block.height,
                   dt.strftime(last_ingested_block.time, '%F %T')))
        args.start_index = next_block
    print('-' * 58)
    cluster.shutdown()

    if args.info:
        raise SystemExit(0)

    # handle negative end index
    if args.end_index < 0:
        end_index = len(chain) + args.end_index + 1
    else:
        end_index = args.end_index + 1
    block_range = chain[args.start_index:end_index]

    if args.start_index >= len(chain) and args.continue_ingest:
        print('No blocks/transactions to ingest')
        raise SystemExit(0)

    if args.start_index >= len(chain):
        print('Error: --start_index argument must be smaller than %d' %
              len(chain))
        raise SystemExit(1)

    if args.start_index >= end_index:
        print('Error: --start_index argument must be smaller than '
              '--end_index argument')
        raise SystemExit(1)

    if args.concurrency < 1:
        print('Error: --concurrency argument must be strictly positive.')
        raise SystemExit(1)

    if not args.num_chunks:
        args.num_chunks = args.num_proc

    if args.prev_day:
        tstamp_today = time.mktime(dt.today().date().timetuple())
        block_tstamps = block_range.time.astype(dt)/1e9
        v = np.where(block_tstamps < tstamp_today)[0]
        if len(v):
            last_index = np.max(v)
            last_height = block_range[last_index].height
            if last_height != chain[args.end_index].height:
                print('Discarding blocks %d ... %d' %
                      (last_height + 1, chain[args.end_index].height))
                block_range = chain[args.start_index:(last_height + 1)]
        else:
            print('No blocks to ingest.')
            raise SystemExit

    num_blocks = len(block_range)
    block_index_range = (block_range[0].height, block_range[-1].height + 1)
    tx_index_range = (block_range[0].txes[0].index,
                      block_range[-1].txes[-1].index + 1)
    num_tx = tx_index_range[1] - tx_index_range[0] + 1

    tables = check_tables_arg(args.tables)
    print('-' * 58)

    cluster = Cluster(args.db_nodes, port=args.db_port)

    # transactions
    if 'tx' in tables:

        print('Transactions ({:,.0f} tx)'.format(num_tx))
        print('{:,.0f} <= tx id < {:,.0f}'.format(*tx_index_range))
        cql_str = '''INSERT INTO transaction
                     (tx_id_group, tx_id, tx_hash, block_id,
                      timestamp, coinbase, total_input, total_output,
                      inputs, outputs, coinjoin)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        qm = TxQueryManager(cluster, args.keyspace, chain, cql_str,
                            args.num_proc, args.num_chunks, args.concurrency)
        qm.execute(TxQueryManager.insert, tx_index_range)
        qm.close_pool()

        print('Transactions by tx_hash lookup table')
        cql_str = '''INSERT INTO transaction_by_tx_prefix
                     (tx_prefix, tx_hash, tx_id)
                     VALUES (?, ?, ?)'''
        qm = TxLookupQueryManager(cluster, args.keyspace, chain, cql_str,
                                  args.num_proc, args.num_chunks,
                                  args.concurrency)
        qm.execute(TxLookupQueryManager.insert_lookup_table, tx_index_range)
        qm.close_pool()

    # block transactions
    if 'block_tx' in tables:
        print('Block transactions ({:,.0f} blocks)'.format(num_blocks))
        print('{:,.0f} <= block index < {:,.0f}'.format(*block_index_range))
        cql_str = '''INSERT INTO block_transactions
                     (block_id_group, block_id, txs) VALUES (?, ?, ?)'''
        qm = BlockTxQueryManager(cluster, args.keyspace, chain, cql_str,
                                 args.num_proc, args.num_chunks,
                                 args.concurrency)
        qm.execute(BlockTxQueryManager.insert, block_index_range)
        qm.close_pool()

    # blocks
    if 'block' in tables:
        print('Blocks ({:,.0f} blocks)'.format(num_blocks))
        print('{:,.0f} <= block index < {:,.0f}'.format(*block_index_range))
        cql_str = '''INSERT INTO block
                     (block_id_group, block_id, block_hash,
                      timestamp, no_transactions)
                     VALUES (?, ?, ?, ?, ?)'''
        generator = (block_summary(x, int(BLOCK_BUCKET_SIZE))
                     for x in block_range)
        insert(cluster, args.keyspace, cql_str, generator, args.concurrency)

    # summary statistics
    if 'stats' in tables:
        insert_summary_stats(cluster,
                             args.keyspace,
                             chain[block_range[-1].height])

    # configuration details
    session = cluster.connect(args.keyspace)
    cql_str = '''INSERT INTO configuration
                 (id, block_bucket_size, tx_prefix_length, tx_bucket_size)
                 VALUES (%s, %s, %s, %s)'''
    session.execute(cql_str,
                    (args.keyspace,
                     int(BLOCK_BUCKET_SIZE),
                     int(TX_HASH_PREFIX_LENGTH),
                     int(TX_BUCKET_SIZE)))

    if 'tx' in tables and args.bip30_fix:  # handle BTC duplicate tx_hash issue
        print("Applying fix for BIP30 (duplicate tx hashes)")
        session = cluster.connect(args.keyspace)
        cql_str = '''INSERT INTO transaction_by_tx_prefix
                     (tx_prefix, tx_hash, tx_id) VALUES (?, ?, ?)'''
        prep_stmt = session.prepare(cql_str)
        upsert_btc_duplicate_hashes(session, prep_stmt)

    cluster.shutdown()


if __name__ == '__main__':
    main()
