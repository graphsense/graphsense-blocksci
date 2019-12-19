#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''Utility script to check value in blocks and exchange_rates table'''

from argparse import ArgumentParser
from datetime import datetime
import time
from cassandra.cluster import Cluster


def main():
    parser = ArgumentParser(description='',
                            epilog='GraphSense - http://graphsense.info')
    parser.add_argument('-c', '--cassandra', dest='cassandra',
                        default='localhost', metavar='CASSANDRA_HOSTS',
                        help='Cassandra nodes (comma separated list of nodes'
                             '; default "localhost")')
    parser.add_argument('-k', '--keyspace', dest='keyspace',
                        required=True,
                        help='Cassandra keyspace')

    args = parser.parse_args()

    cluster = Cluster(args.cassandra.split(','))
    session = cluster.connect(args.keyspace)
    session.default_timeout = 60

    cql_str = '''SELECT height FROM block'''
    res = session.execute(cql_str)
    max_val = 0
    for i, row in enumerate(res):
        max_val = max(max_val, row[0])
    cql_str = f'''SELECT timestamp FROM block WHERE height={max_val}'''
    res = session.execute(cql_str)
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S',
                              time.gmtime(res[0].timestamp))
    print(f'Max height in blocks table: {max_val} ({timestamp})')
    print(f'#rows in blocks table: %d' % (i + 1))

    cql_str = '''SELECT date FROM exchange_rates'''
    res = session.execute(cql_str)
    max_val = datetime.strptime('1970-01-01', '%Y-%m-%d')
    for i, row in enumerate(res):
        max_val = max(max_val, datetime.strptime(row[0], '%Y-%m-%d'))
    print('Max date in exchange_rates table: %s' % max_val)
    print('#rows in exchange_rates table: %d' % (i + 1))
    cluster.shutdown()


if __name__ == '__main__':
    main()
