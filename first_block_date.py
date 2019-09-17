#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''Script to retrieve the first block on a specified day.'''

import argparse
import datetime

import numpy as np
import blocksci


def valid_date(date_str):
    '''Check if date string is in ISO-format.'''
    try:
        return datetime.datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        msg = f'Not a valid date: "{date_str}".'
        raise argparse.ArgumentTypeError(msg)


def get_first_block(chain, date):
    '''Return first block on a specified date.'''
    index = np.where(chain[:].timestamp < date.timestamp())
    return index[0][-1] + 1


def main():
    '''Main function.'''
    parser = argparse.ArgumentParser(
        description='Retrieve first block on a specified date',
        epilog='GraphSense - http://graphsense.info')
    parser.add_argument('-c', '--config', dest='blocksci_config',
                        required=True,
                        help='BlockSci configuration file')
    parser.add_argument('-d', '--date', dest='date',
                        required=True, type=valid_date,
                        help='Date in ISO-format YYYY-MM-DD')

    args = parser.parse_args()

    chain = blocksci.Blockchain(args.blocksci_config)
    first_block = get_first_block(chain, args.date)
    print(first_block)


if __name__ == '__main__':
    main()
