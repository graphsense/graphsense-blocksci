from argparse import ArgumentParser
from multiprocessing import Pool, Value
import time

import blocksci
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement


class TxQueryManager(object):

    # chosen to match the default in execute_concurrent_with_args
    concurrency = 100
    counter = Value("d", 0)

    def __init__(self, cluster, keyspace, chain, process_count=1):
        self.process_count = process_count
        self.pool = Pool(processes=process_count,
                         initializer=self._setup,
                         initargs=(cluster, chain, keyspace))

    @classmethod
    def _setup(cls, cluster, chain, keyspace):
        cls.chain = chain
        #cls.cluster = Cluster(cluster_nodes)
        cls.session = cluster.connect()
        cls.session.default_timeout = 60
        cls.session.set_keyspace(keyspace)
        cls.session.default_consistency_level = ConsistencyLevel.LOCAL_ONE

        cql_str = """INSERT INTO transaction
                         (tx_prefix, tx_hash, height, timestamp, coinbase,
                          total_input, total_output, inputs, outputs)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"""
        cls.prepared_stmt = cls.session.prepare(cql_str)

    def close_pool(self):
        self.pool.close()
        self.pool.join()

    def insert(self, params):
        self.pool.map(_multiprocess_insert,
                      chunk(params, self.process_count))
                      #self.concurrency)

    @classmethod
    def insert_tx(cls, params):

        idx_start, idx_end = params

        batch_size = 1000
        count = 0
        for index in range(idx_start, idx_end):

            tx = blocksci.Tx(index, cls.chain)

            if count == 0:
                batch_stmt = BatchStatement()

            batch_stmt.add(cls.prepared_stmt, tx_summary(tx))

            count += 1

            if (count % batch_size) == 0:
                cls.session.execute(batch_stmt)
                count = 0
                batch_stmt.clear()

                print("#tx {:,.0f}".format(cls.counter.value), end="\r")
                with cls.counter.get_lock():
                    cls.counter.value += batch_size


def _multiprocess_insert(params):
    return TxQueryManager.insert_tx(params)


def insert_blocks(cluster, keyspace, chain):
    session = cluster.connect(keyspace)
    session.default_timeout = 60
    session.default_consistency_level = ConsistencyLevel.LOCAL_ONE

    cql_str = """INSERT INTO block
                     (height, block_hash, timestamp, no_transactions)
                 VALUES (?, ?, ?, ?)"""
    prepared_stmt = session.prepare(cql_str)
    count = 0

    batch_size = 1000
    last_block = len(chain)
    print("last_block %d:" % last_block)

    for index in range(0, last_block, batch_size):
        batch_stmt = BatchStatement()
        values = block_batch(chain[index:(index + batch_size)])
        batch_stmt.add_all([prepared_stmt]*batch_size, values)
        session.execute_async(batch_stmt)
        batch_stmt.clear()
        print("%.1f%%" % (index/last_block*100), end="\r")
    else:
        print()

    session.cluster.shutdown()


def chunk(n, k):
    '''Split the number range [0, n] into k evenly sized chunks'''

    assert 0 < k <= n
    s, r = divmod(n, k)
    t = s + 1
    return ([(p, p+t) for p in range(0, r*t, t)] +
            [(p, p+s) for p in range(r*t, n, s)])


def addr_str(addr_obj):
    if addr_obj.type == blocksci.address_type.multisig:
        res = [x.address_string for x in addr_obj.addresses]
    elif addr_obj.type == blocksci.address_type.nonstandard:
        res = ['nonstandard']
    elif addr_obj.type == blocksci.address_type.nulldata:
        res = ['nulldata']
    else:
        res = [addr_obj.address_string]
    return(res)


def block_batch(block):
    return zip(block.height,
               (x.tobytes() for x in block.hash),
               block.timestamp,
               (len(x) for x in block))


def tx_stats(tx):
    return (bytearray.fromhex(str(tx.hash)),
            len(tx.inputs),
            len(tx.outputs),
            tx.input_value,
            tx.output_value)


def tx_summary(tx):
   tx_inputs = zip([addr_str(x.address) for x in tx.inputs.all],
                   [x.value for x in tx.inputs.all])
   tx_outputs = zip([addr_str(x.address) for x in tx.outputs.all],
                    [x.value for x in tx.outputs.all])
   return (str(tx.hash)[:5],
           bytearray.fromhex(str(tx.hash)),
           tx.block_height,
           int(tx.block_time.timestamp()),
           tx.is_coinbase,
           tx.input_value,
           tx.output_value,
           list(tx_inputs),
           list(tx_outputs))


def main():
    parser = ArgumentParser()
    parser.add_argument("-c", "--cassandra", dest="cassandra",
                        help="cassandra nodes (comma separated list of nodes)",
                        default="localhost")
    parser.add_argument("-k", "--keyspace", dest="keyspace",
                        required=True,
                        help="keyspace to import data to")
    parser.add_argument("-b", "--blocksci_data", dest="blocksci_data",
                        required=True,
                        help="source directory for raw JSON block dumps")
    parser.add_argument("-p", "--processes", dest="num_proc",
                        type=int, default=1,
                        help="number of processes")

    args = parser.parse_args()

    chain = blocksci.Blockchain(args.blocksci_data)
    num_tx = chain[-6].txes.all[-1].index

    cluster = Cluster(args.cassandra.split(","))

    qm = TxQueryManager(cluster, args.keyspace, chain, args.num_proc)
    start = time.time()
    print("\nIngest {:,.0f} transactions".format(num_tx))
    qm.insert(num_tx)
    delta = time.time() - start
    print("\n%.1fs" % delta)

    print("Ingest %d blocks" % len(chain[:-6]))
    insert_blocks(cluster, args.keyspace, chain[:-6])


if __name__ == "__main__":
    main()
