from abc import ABC
from argparse import ArgumentParser
from datetime import date
from functools import wraps
from itertools import islice
from multiprocessing import Pool, Value
import time

import blocksci
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement


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
  'address_type.witness_scripthash': 9
}


def timing(f):
    @wraps(f)
    def wrap(*args, **kw):
        t1 = time.time()
        result = f(*args, **kw)
        t2 = time.time()
        print('\n... %.1f sec\n' % (t2 - t1))
        return result
    return wrap


class QueryManager(ABC):

    # chosen to match the default in execute_concurrent_with_args
    concurrency = 100
    counter = Value('d', 0)

    def __init__(self, cluster, keyspace, chain, cql_str, process_count=1):
        self.process_count = process_count
        self.pool = Pool(processes=process_count,
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
        self.pool.map(fun, chunk(params, self.process_count))

    @classmethod
    def insert(cls, params):
        return


class TxQueryManager(QueryManager):

    @classmethod
    def insert(cls, params):

        idx_start, idx_end = params

        batch_size = 1000

        count = 0
        batch_stmt = BatchStatement()
        for index in range(idx_start, idx_end):

            tx = blocksci.Tx(index, cls.chain)
            batch_stmt.add(cls.prepared_stmt, tx_summary(tx))
            count += 1

            if (count % batch_size) == 0:
                cls.session.execute(batch_stmt)
                count = 0
                batch_stmt.clear()

                print('#tx {:,.0f}'.format(cls.counter.value), end='\r')
                with cls.counter.get_lock():
                    cls.counter.value += batch_size
        else:
            cls.session.execute(batch_stmt)


class BlockTxQueryManager(QueryManager):

    @classmethod
    def insert(cls, params):

        idx_start, idx_end = params

        batch_size = 50

        count = 0
        batch_stmt = BatchStatement()
        for index in range(idx_start, idx_end):

            block = cls.chain[index]
            block_tx = [block.height, [tx_stats(x) for x in block.txes]]
            batch_stmt.add(cls.prepared_stmt, block_tx)

            count += 1

            if (count % batch_size) == 0:
                cls.session.execute(batch_stmt)
                count = 0
                batch_stmt.clear()

                print('#blocks {:,.0f}'.format(cls.counter.value), end='\r')
                with cls.counter.get_lock():
                    cls.counter.value += batch_size
        else:
            cls.session.execute(batch_stmt)


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
        if (count % 10000) == 0:
            print('#blocks {:,.0f}'.format(count), end='\r')
        count += batch_size


def take(n, iterable):
    '''Return first n items of the iterable as a list'''
    return list(islice(iterable, n))


def chunk(val_range, k):
    '''Split the number range val_range=[n1, n2] into k evenly sized chunks'''

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
    else:
        res = [addr_obj.address_string]
    return(res)


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
    tx_inputs = [tx_io_summary(x) for x in tx.inputs.all]
    tx_outputs = [tx_io_summary(x) for x in tx.outputs.all]
    return (str(tx.hash)[:5],
            bytearray.fromhex(str(tx.hash)),
            tx.index,
            tx.block_height,
            int(tx.block_time.timestamp()),
            tx.is_coinbase,
            tx.input_value,
            tx.output_value,
            list(tx_inputs),
            list(tx_outputs))


def main():
    parser = ArgumentParser(description='Export dumped BlockSci data '
                                        'to Apache Cassandra',
                            epilog='GraphSense - http://graphsense.info')
    parser.add_argument('-b', '--blocksci_data', dest='blocksci_data',
                        required=True,
                        help='directory containing parsed BlockSci data')
    parser.add_argument('-c', '--cassandra', dest='cassandra',
                        default='localhost', metavar='CASSANDRA_HOSTS',
                        help='Cassandra nodes (comma separated list of nodes'
                             '; default "localhost")')
    parser.add_argument('-k', '--keyspace', dest='keyspace',
                        required=True,
                        help='Cassandra keyspace')
    parser.add_argument('-p', '--processes', dest='num_proc',
                        type=int, default=1,
                        help='number of processes (default 1)')
    parser.add_argument('-s', '--start_index', dest='start_index',
                        type=int, default=0,
                        help='start index of the blocks to export '
                             '(default 0)')
    parser.add_argument('-e', '--end_index', dest='end_index',
                        type=int, default=-1,
                        help='only blocks with height smaller than this '
                             'value are included; a negative index counts '
                             'back from the end (default -1)')

    args = parser.parse_args()

    chain = blocksci.Blockchain(args.blocksci_data)
    block_range = chain[args.start_index:args.end_index]
    num_blocks = len(block_range)
    block_index_range = (block_range[0].height, block_range[-1].height + 1)
    tx_index_range = (block_range[block_index_range[0]].txes.all[0].index,
                      block_range[block_index_range[1] - 1].txes.all[-1].index + 1)
    num_tx = tx_index_range[1] - tx_index_range[0] + 1

    cluster = Cluster(args.cassandra.split(','))

    # block transactions
    print('Block transactions ({:,.0f} blocks)'.format(num_blocks))
    cql_str = '''INSERT INTO block_transactions (height, txs) VALUES (?, ?)'''
    qm = BlockTxQueryManager(cluster, args.keyspace, chain, cql_str,
                             args.num_proc)
    qm.execute(BlockTxQueryManager.insert, block_index_range)
    qm.close_pool()

    # transactions
    print('Transactions ({:,.0f} tx)'.format(num_tx))
    cql_str = '''INSERT INTO transaction
                 (tx_prefix, tx_hash, tx_index, height, timestamp,
                  coinbase, total_input, total_output, inputs, outputs)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    qm = TxQueryManager(cluster, args.keyspace, chain, cql_str,
                        args.num_proc)
    qm.execute(TxQueryManager.insert, tx_index_range)
    qm.close_pool()

    # blocks
    print('Ingest {:,.0f} blocks'.format(num_blocks))
    cql_str = '''INSERT INTO block
                 (height, block_hash, timestamp, no_transactions)
                 VALUES (?, ?, ?, ?)'''
    generator = (block_summary(x) for x in block_range)
    insert(cluster, args.keyspace, cql_str, generator, 1000)

    # exchange rates
    print('Exchange rates')
    cql_str = '''INSERT INTO exchange_rates (height, eur, usd)
                 VALUES (?, ?, ?)'''
    cc_eur = blocksci.currency.CurrencyConverter(currency='EUR')
    cc_usd = blocksci.currency.CurrencyConverter(currency='USD')
    generator = ((elem.height,
                  cc_usd.exchangerate(date.fromtimestamp(elem.timestamp)),
                  cc_eur.exchangerate(date.fromtimestamp(elem.timestamp)))
                 for elem in block_range
                 if date.fromtimestamp(elem.timestamp) < date.today())
    insert(cluster, args.keyspace, cql_str, generator, 1000)
    cluster.shutdown()


if __name__ == '__main__':
    main()
