from argparse import ArgumentParser
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
    for row in res:
        max_val = max(max_val, row[0])
    print(max_val)
    cluster.shutdown()


if __name__ == '__main__':
    main()
