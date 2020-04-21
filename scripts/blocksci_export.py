#!/usr/bin/env python3

from abc import ABC
from argparse import ArgumentParser
from datetime import datetime
from functools import wraps
from itertools import islice
from multiprocessing import Pool, Value
import time


from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
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


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        t1 = datetime.now()
        result = f(*args, **kw)
        t2 = datetime.now()
        print('\n... %s\n' % str(t2 - t1))
        return result
    return wrap


class QueryManager(ABC):

    # chosen to match the default in execute_concurrent_with_args
    concurrency = 100
    counter = Value('d', 0)

    def __init__(self, cluster, keyspace, chain, cql_str,
                 num_proc=1, num_chunks=None):
        if not num_chunks:
            num_chunks = num_proc
        self.num_proc = num_proc
        self.num_chunks = num_chunks
        self.pool = Pool(processes=num_proc,
                         initializer=self._setup,
                         initargs=(cluster, chain, keyspace, cql_str))

    @classmethod
    def _setup(cls, cluster, chain, keyspace, cql_str):
        cls.chain = chain
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

        batch_size = 500
        batch_stmt = BatchStatement()

        for index in range(idx_start, idx_end, batch_size):

            curr_batch_size = min(batch_size, idx_end - index)
            for i in range(0, curr_batch_size):
                tx = blocksci.Tx(index + i, cls.chain)
                batch_stmt.add(cls.prepared_stmt, tx_summary(tx))

            try:
                cls.session.execute(batch_stmt)
            except Exception as e:
                # ingest single transactions if batch ingest fails
                # (batch too large error)
                print(e)
                for i in range(0, curr_batch_size):
                    while True:
                        try:
                            tx = blocksci.Tx(index + i, cls.chain)
                            cls.session.execute(cls.prepared_stmt,
                                                tx_summary(tx))
                        except Exception as e:
                            print(e)
                            continue
                        break
            batch_stmt.clear()

            with cls.counter.get_lock():
                cls.counter.value += curr_batch_size
            print('#tx {:,.0f}'.format(cls.counter.value), end='\r')


class BlockTxQueryManager(QueryManager):

    counter = Value('d', 0)

    @classmethod
    def insert(cls, params):

        idx_start, idx_end = params

        batch_size = 25
        batch_stmt = BatchStatement()

        for index in range(idx_start, idx_end, batch_size):

            curr_batch_size = min(batch_size, idx_end - index)
            for i in range(0, curr_batch_size):
                block = cls.chain[index + i]
                block_tx = [block.height, [tx_stats(x) for x in block.txes]]
                batch_stmt.add(cls.prepared_stmt, block_tx)

            try:
                cls.session.execute(batch_stmt)
            except Exception as e:
                # ingest single blocks batch ingest fails
                # (batch too large error)
                print(e)
                for i in range(0, curr_batch_size):
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
            batch_stmt.clear()

            with cls.counter.get_lock():
                cls.counter.value += curr_batch_size
            print('#blocks {:,.0f}'.format(cls.counter.value), end='\r')


@timing
def insert(cluster, keyspace, cql_stmt, generator, batch_size):
    session = cluster.connect(keyspace)
    session.default_timeout = 60
    session.default_consistency_level = ConsistencyLevel.LOCAL_ONE
    prepared_stmt = session.prepare(cql_stmt)
    batch_stmt = BatchStatement()

    values = take(batch_size, generator)
    count = 0
    while values:
        batch_stmt.add_all([prepared_stmt]*batch_size, values)
        session.execute(batch_stmt)

        values = take(batch_size, generator)
        batch_stmt.clear()
        if (count % 1e3) == 0:
            print('#blocks {:,.0f}'.format(count), end='\r')
        count += batch_size


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


def block_summary(block):
    return (block.height,
            bytearray.fromhex(str(block.hash)),
            block.timestamp,
            len(block))


def tx_stats(tx):
    return (bytearray.fromhex(str(tx.hash)),
            len(tx.inputs),
            len(tx.outputs),
            tx.input_value,
            tx.output_value)


def tx_io_summary(x):
    return [addr_str(x.address), x.value, address_type[repr(x.address_type)]]


def tx_summary(tx):
    tx_inputs = [tx_io_summary(x) for x in tx.inputs]
    tx_outputs = [tx_io_summary(x) for x in tx.outputs]
    return (str(tx.hash)[:5],
            bytearray.fromhex(str(tx.hash)),
            tx.index,
            tx.block_height,
            int(tx.block_time.timestamp()),
            tx.is_coinbase,
            tx.input_value,
            tx.output_value,
            list(tx_inputs),
            list(tx_outputs),
            blocksci.heuristics.is_coinjoin(tx))


def insert_summary_stats(cluster, keyspace, last_block):
    total_blocks = last_block.height + 1
    total_txs = last_block.txes[-1].index + 1
    timestamp = last_block.timestamp

    session = cluster.connect(keyspace)
    cql_str = '''INSERT INTO summary_statistics
                 (id, timestamp, no_blocks, no_txs)
                 VALUES (%s, %s, %s, %s)'''
    session.execute(cql_str, (keyspace, timestamp, total_blocks, total_txs))


def main():
    parser = ArgumentParser(description='Export dumped BlockSci data '
                                        'to Apache Cassandra',
                            epilog='GraphSense - http://graphsense.info')
    parser.add_argument('-c', '--config', dest='blocksci_config',
                        required=True,
                        help='BlockSci configuration file')
    parser.add_argument('-d', '--db_nodes', dest='db_nodes', nargs='+',
                        default='localhost', metavar='DB_NODE',
                        help='list of Cassandra nodes; default "localhost")')
    parser.add_argument('-k', '--keyspace', dest='keyspace',
                        required=True,
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
                             'available for the current day.')
    parser.add_argument('--start_index', dest='start_index',
                        type=int, default=0,
                        help='start index of the blocks to export '
                             '(default 0)')
    parser.add_argument('--end_index', dest='end_index',
                        type=int, default=-1,
                        help='only blocks with height smaller than '
                             'this value are included; a negative index '
                             'counts back from the end (default -1)')
    parser.add_argument('--blocks', action='store_true',
                        help='ingest only into the blocks table')
    parser.add_argument('--block_tx', action='store_true',
                        help='ingest only into the block_transactions table')
    parser.add_argument('--tx', action='store_true',
                        help='ingest only into the transactions table')
    parser.add_argument('--statistics', action='store_true',
                        help='ingest only into the summary statistics table')

    args = parser.parse_args()

    chain = blocksci.Blockchain(args.blocksci_config)
    print('Last parsed block: %d (%s)' %
          (chain[-1].height, datetime.strftime(chain[-1].time, '%F %T')))
    block_range = chain[args.start_index:args.end_index]

    if args.start_index >= len(chain):
        print('Error: --start_index argument must be smaller than %d' %
              len(chain))
        raise SystemExit

    if not args.num_chunks:
        args.num_chunks = args.num_proc

    if args.prev_day:
        tstamp_today = time.mktime(datetime.today().date().timetuple())
        block_tstamps = block_range.time.astype(datetime)/1e9
        v = np.where(block_tstamps < tstamp_today)[0]
        if len(v):
            last_index = np.max(v)
            last_height = block_range[last_index].height
            if last_height + 1 != chain[args.end_index].height:
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

    cluster = Cluster(args.db_nodes)

    all_tables = not (args.blocks or args.block_tx or
                      args.tx or args.statistics)

    # transactions
    if all_tables or args.tx:

        print('Transactions ({:,.0f} tx)'.format(num_tx))
        print('tx index: {:,.0f} -- {:,.0f}'.format(*tx_index_range))
        cql_str = '''INSERT INTO transaction
                     (tx_prefix, tx_hash, tx_index, height,
                      timestamp, coinbase, total_input, total_output,
                      inputs, outputs, coinjoin)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        qm = TxQueryManager(cluster, args.keyspace, chain, cql_str,
                            args.num_proc, args.num_chunks)
        qm.execute(TxQueryManager.insert, tx_index_range)
        qm.close_pool()

    # block transactions
    if all_tables or args.block_tx:
        print('Block transactions ({:,.0f} blocks)'.format(num_blocks))
        print('block index: {:,.0f} -- {:,.0f}'.format(*block_index_range))
        cql_str = '''INSERT INTO block_transactions
                     (height, txs) VALUES (?, ?)'''
        qm = BlockTxQueryManager(cluster, args.keyspace, chain, cql_str,
                                 args.num_proc, args.num_chunks)
        qm.execute(BlockTxQueryManager.insert, block_index_range)
        qm.close_pool()

    # blocks
    if all_tables or args.blocks:
        print('Blocks ({:,.0f} blocks)'.format(num_blocks))
        print('block index: {:,.0f} -- {:,.0f}'.format(*block_index_range))
        cql_str = '''INSERT INTO block
                     (height, block_hash, timestamp, no_transactions)
                     VALUES (?, ?, ?, ?)'''
        generator = (block_summary(x) for x in block_range)
        insert(cluster, args.keyspace, cql_str, generator, 1000)

    if all_tables or args.statistics:
        insert_summary_stats(cluster,
                             args.keyspace,
                             chain[block_range[-1].height])

    cluster.shutdown()


if __name__ == '__main__':
    main()
