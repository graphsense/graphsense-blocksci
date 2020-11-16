#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''Script to setup GraphSense raw keyspace.'''

from argparse import ArgumentParser
from cassandra.cluster import Cluster

DEFAULT_TIMEOUT = 60
KEYSPACE_PLACEHOLDER = 'graphsense'


class StorageError(Exception):
    '''Class for Cassandra-related errors'''

    def __init__(self, message, nested_exception=None):
        super().__init__('Cassandra Error: ' + message)
        self.nested_exception = nested_exception

    def __str__(self):
        msg = super(StorageError, self).__str__()
        if self.nested_exception:
            msg = msg + '\nError Details: ' + str(self.nested_exception)
        return msg


class Cassandra:
    '''Cassandra connector'''

    def __init__(self, db_nodes):
        self.db_nodes = db_nodes
        self.cluster = None
        self.session = None

    def connect(self):
        '''Connect to given Cassandra cluster nodes.'''
        self.cluster = Cluster(self.db_nodes)
        try:
            self.session = self.cluster.connect()
            self.session.default_timeout = DEFAULT_TIMEOUT
        except Exception as e:
            raise StorageError(f'Cannot connect to {self.db_nodes}', e)

    def has_keyspace(self, keyspace):
        '''Check whether a given keyspace is present in the cluster.'''
        if not self.session:
            raise StorageError('Session not available. Call connect() first')
        try:
            query = 'SELECT keyspace_name FROM system_schema.keyspaces'
            result = self.session.execute(query)
            keyspaces = [row.keyspace_name for row in result]
            return keyspace in keyspaces
        except Exception as e:
            raise StorageError(f'Error when executing query:\n{query}', e)

    def setup_keyspace(self, keyspace, schema_file):
        '''Setup keyspace and tables.'''
        if not self.session:
            raise StorageError('Session not available, call connect() first')

        with open(schema_file, 'r') as fh:
            schema = fh.read()

        # replace keyspace name placeholder in CQL schema script
        schema = schema.replace(KEYSPACE_PLACEHOLDER, keyspace)

        statements = schema.split(';')
        for stmt in statements:
            stmt = stmt.strip()
            if len(stmt) > 0:
                self.session.execute(stmt + ';')

    def close(self):
        '''Closes the cassandra cluster connection.'''
        self.cluster.shutdown()


def main():
    '''Main function.'''

    parser = ArgumentParser(description='Create raw keyspace in Cassandra',
                            epilog='GraphSense - http://graphsense.info')
    parser.add_argument('-d', '--db_nodes', dest='db_nodes', nargs='+',
                        default='localhost', metavar='DB_NODE',
                        help='list of Cassandra nodes (default "localhost")')
    parser.add_argument('-k', '--keyspace', dest='keyspace_name',
                        required=True, metavar='KEYSPACE',
                        help='name of GraphSense raw keyspace')
    parser.add_argument('-s', '--schema', dest='schema_template',
                        required=True, metavar='CQL_SCHEMA',
                        help='Cassandra schema for GraphSense raw keyspace')
    args = parser.parse_args()

    cassandra = Cassandra(args.db_nodes)
    cassandra.connect()
    if not cassandra.has_keyspace(args.keyspace_name):
        cassandra.setup_keyspace(args.keyspace_name, args.schema_template)
        print(f'Success: Keyspace "{args.keyspace_name}" created.')
    else:
        print(f'Error: Keyspace "{args.keyspace_name}" already exists.')
    cassandra.close()


if __name__ == '__main__':
    main()
