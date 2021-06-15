#!/usr/bin/env python3
# coding: utf-8
'''Script to fetch cryptocurrency exchange rates from CoinMarketCap'''

from argparse import ArgumentParser
from datetime import date, datetime, timedelta
import json

from cassandra.cluster import Cluster
import pandas as pd
import requests


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


def historical_coin_url(symbol, start, end):
    return 'https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/' + \
           f'historical?symbol={symbol}&convert=USD' + \
           f'&time_start={start}&time_end={end}'


def parse_historical_coin_response(response):
    '''Parse historical exchange rates (JSON) from CoinMarketCap'''

    json_data = json.loads(response.content)
    json_data = [[elem['time_close'][:10], elem['quote']['USD']['close']]
                 for elem in json_data['data']['quotes']]

    return pd.DataFrame(json_data, columns=['date', 'USD'])


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

    user_agent = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/87.0.4280.88 Safari/537.36')
    headers = {'User-Agent': user_agent}

    start_date = date.fromisoformat(start) + timedelta(days=-1)
    end_date = date.fromisoformat(end)
    url = historical_coin_url(crypto_currency, start_date, end_date)

    print(f'Fetching {crypto_currency} exchange rates\n{url}')
    response = requests.get(url, headers=headers)
    df = parse_historical_coin_response(response)

    print(f'Last record: {df.date.tolist()[-1]}')
    return df


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
    parser.add_argument('--fiat_currencies', dest='fiat_currencies', nargs='+',
                        default=['USD', 'EUR'], metavar='FIAT_CURRENCY',
                        help='list of fiat currencies;' +
                             'default ["USD", "EUR"]')
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
    parser.add_argument('--cryptocurrency', dest='cryptocurrency',
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
    print(f'Target fiat currencies: {args.fiat_currencies}')

    # fetch crypto currency exchange rates in USD
    crypto_df = fetch_crypto_exchange_rates(start, end, crypto_currency)

    fx_rates = fetch_fx_rates(args.fiat_currencies)
    # query conversion rates and merge converted values in exchange rates
    exchange_rates = crypto_df
    for fiat_currency in set(args.fiat_currencies) - set(['USD']):
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
    if 'USD' not in args.fiat_currencies:
        exchange_rates.drop('USD', axis=1, inplace=True)
    exchange_rates['fiat_values'] = exchange_rates \
        .drop('date', axis=1) \
        .to_dict(orient='records')
    exchange_rates.drop(args.fiat_currencies, axis=1, inplace=True)

    print(f'Inserted rates for {len(exchange_rates)} days')
    insert_exchange_rates(session, keyspace, table, exchange_rates)

    cluster.shutdown()


if __name__ == '__main__':
    main()
