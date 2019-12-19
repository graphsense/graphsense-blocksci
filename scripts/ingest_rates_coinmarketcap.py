#!/usr/bin/env python3
# coding: utf-8
'''Script to fetch cryptocurrency exchange rates from CoinMarketCap'''

from argparse import ArgumentParser
from datetime import date, datetime, timedelta
import json

import bs4
from cassandra.cluster import Cluster
import pandas as pd
import requests


class ExchangeRateParsingError(Exception):
    pass


def fetch_fx_rates(symbol_list):
    '''Fetch and preprocess FX rates from ECB.'''

    FX_URL = 'https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip'
    print(f'Fetching conversion rates for FIAT currencies:\n{FX_URL}')
    rates_eur = pd.read_csv(FX_URL)  # exchange rates based on EUR
    rates_eur = rates_eur.iloc[:, :-1]  # remove empty last column
    rates_eur['EUR'] = 1.
    # convert to values based on USD
    rates_usd = rates_eur[symbol_list].div(rates_eur.USD, axis=0)
    rates_usd['date'] = rates_eur.Date
    print(f'Last record: {rates_usd.date.tolist()[0]}')
    return rates_usd


def historical_coin_url(slug, start, end):
    start = start.replace('-', '')
    end = end.replace('-', '')
    return f'https://coinmarketcap.com/currencies/{slug}/historical-data/' + \
           f'?start={start}&end={end}'


def parse_all_response(resp):
    '''Extract and parse summary table from CoinMarketCap'''

    soup = bs4.BeautifulSoup(resp.text, 'lxml')
    soup_id = soup.find(id='__NEXT_DATA__')

    json_data = json.loads(list(soup_id)[0])['props']['initialState']
    json_data = json_data['cryptocurrency']['listingLatest']['data']
    df = pd.DataFrame([{'symbol': elem['symbol'], 'slug':elem['slug']}
                       for elem in json_data])
    return df


def parse_historical_coin_response(resp):
    '''Extract and parse historical exchange rates (JSON) from CoinMarketCap'''

    soup = bs4.BeautifulSoup(resp.text, 'lxml')
    soup_id = soup.find(id='__NEXT_DATA__')

    json_data = json.loads(list(soup_id)[0])['props']['initialState']
    ohlcv_hist = json_data['cryptocurrency']['ohlcvHistorical']

    key = list(ohlcv_hist)[0]
    if ohlcv_hist[key]:
        df = pd.DataFrame([elem['quote']['USD']
                           for elem in ohlcv_hist[key]['quotes']])
        df['date'] = pd.to_datetime(df.timestamp).dt.strftime('%Y-%m-%d')
        df = df[['date', 'close']]
        df.rename(columns={'close': 'USD'}, inplace=True)
    else:
        raise ExchangeRateParsingError
    return df


def lookup_slug(all_df, symbol):

    if not symbol.isupper():
        symbol = symbol.upper()
    df_row = all_df.loc[all_df['symbol'] == symbol]
    if df_row.empty:
        return None
    slug = df_row['slug'].tolist()
    if len(slug) > 1:
        raise ExchangeRateParsingError('Found more than one possible slugs')
    return slug[0]


def query_required_currencies(session, keyspace, table):
    '''Fetch list of FIAT currencies from Cassandra table.'''

    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    session.row_factory = pandas_factory

    query = f'''SELECT column_name FROM system_schema.columns
                WHERE keyspace_name = '{keyspace}'
                AND table_name = '{table}';'''
    result = session.execute(query)
    df = result._current_rows
    currencies = [symbol.upper() for symbol in list(df['column_name'])]
    if 'USD' not in currencies or 'DATE' not in currencies:
        raise ExchangeRateParsingError(
            'USD is mandatory and must be defined in the schema.')
    currencies.remove('DATE')
    currencies.remove('USD')
    return currencies


def query_most_recent_date(session, keyspace, table):
    '''Fetch most recent entry from exchange rates table.'''

    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    session.row_factory = pandas_factory

    query = f'''SELECT date FROM {keyspace}.{table};'''

    result = session.execute(query)
    df = result._current_rows
    if df.empty:
        return None
    df['date'] = df['date'].astype('datetime64')

    largest = df.nlargest(1, 'date').iloc[0]['date']

    return largest.strftime('%Y-%m-%d')


def fetch_crypto_exchange_rates(start, end, crypto_currency):
    '''Fetch cryptocurrency exchange rates from CoinMarketCap.'''

    all_url = 'https://coinmarketcap.com/all/views/all/'
    all_crypto_df = parse_all_response(requests.get(all_url))
    slug = lookup_slug(all_crypto_df, crypto_currency)
    crypto_url = historical_coin_url(slug, start, end)
    print(f'Fetching {crypto_currency} exchange rates:\n{crypto_url}')
    crypto_resp = requests.get(crypto_url)
    crypto_df = parse_historical_coin_response(crypto_resp)
    print(f'Last record: {crypto_df.date.tolist()[0]}')
    return crypto_df


def insert_exchange_rates(session, keyspace, table, exchange_rates_df):
    '''Insert exchange rates into Cassandra table.'''

    colnames = ','.join(exchange_rates_df.columns)
    values = ','.join(['?' for i in range(len(exchange_rates_df.columns))])
    query = f'''INSERT INTO {keyspace}.{table}({colnames}) VALUES ({values})'''
    prepared = session.prepare(query)

    for _, row in exchange_rates_df.iterrows():
        session.execute(prepared, row)


def main():
    '''Main function.'''

    MIN_START = '2009-01-01'
    prev_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')

    parser = ArgumentParser(description='Ingest exchange rates into Cassandra',
                            epilog='GraphSense - http://graphsense.info')
    parser.add_argument('-d', '--db_nodes', dest='db_nodes', nargs='+',
                        default=['localhost'], metavar='DB_NODE',
                        help='list of Cassandra nodes; default "localhost"')
    parser.add_argument('-f', '--force', dest='force', action='store_true',
                        help='do not fetch most recent entries from '
                             'Cassandra and overwrite existing records')
    parser.add_argument('-k', '--keyspace', dest='keyspace',
                        required=True,
                        help='Cassandra keyspace')
    parser.add_argument('-t', '--table', dest='table',
                        default='exchange_rates',
                        help='name of the target exchange rate table')
    parser.add_argument('--start_date', dest='start', type=str,
                        default=MIN_START,
                        help='start date for fetching exchange rates')
    parser.add_argument('--end_date', dest='end', type=str,
                        default=prev_date,
                        help='end date for fetching exchange rates')
    parser.add_argument('-c', '--cryptocurrency', dest='cryptocurrency',
                        type=str, default='BTC', required=True,
                        help='target cryptocurrency')

    args = parser.parse_args()

    cluster = Cluster(args.db_nodes)
    keyspace = args.keyspace
    table = args.table
    session = cluster.connect(keyspace)

    crypto_currency = args.cryptocurrency

    # Default start and end date
    start = args.start
    end = args.end

    print(f'*** Starting exchange rate ingest for {crypto_currency} ***')

    if datetime.fromisoformat(start) < datetime.fromisoformat(MIN_START):
        start = MIN_START

    # query most recent data
    if not args.force:
        most_recent_date = query_most_recent_date(session, keyspace, table)
        if most_recent_date is not None:
            start = most_recent_date

    print(f'Start date: {start}')
    print(f'End date: {end}')

    if datetime.fromisoformat(start) > datetime.fromisoformat(end):
        print('Error: start date after end date.')
        cluster.shutdown()
        raise SystemExit

    # query all required fiat currencies
    try:
        fiat_currencies = query_required_currencies(session, keyspace, table)
        print(f'Target fiat currencies: {fiat_currencies}')
    except ExchangeRateParsingError as err:
        print(f'Error while querying all required fiat currencies: {err}')

    # fetch crypto currency exchange rates in USD
    crypto_df = fetch_crypto_exchange_rates(start, end, crypto_currency)

    fx_rates = fetch_fx_rates(fiat_currencies)
    # query conversion rates and merge converted values in exchange rates
    exchange_rates = crypto_df
    for fiat_currency in fiat_currencies:
        fx_df = fx_rates[['date', fiat_currency]] \
                .rename(columns={fiat_currency: 'fx_rate'})
        merged_df = crypto_df.merge(fx_df, how='left', on='date')
        merged_df['fx_rate'].interpolate(method='linear', inplace=True)
        merged_df['fx_rate'].fillna(method='ffill', inplace=True)
        merged_df['fx_rate'].fillna(method='bfill', inplace=True)
        merged_df[fiat_currency] = merged_df['USD'] * merged_df['fx_rate']
        merged_df = merged_df[['date', fiat_currency]]
        exchange_rates = exchange_rates.merge(merged_df, on='date')

    # insert final exchange rates into Cassandra
    print(f'Inserted rates for {len(exchange_rates)} days')
    insert_exchange_rates(session, keyspace, table, exchange_rates)

    cluster.shutdown()


if __name__ == '__main__':
    main()
