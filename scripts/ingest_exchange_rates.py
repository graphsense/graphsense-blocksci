#!/usr/bin/env python
# coding: utf-8

from argparse import ArgumentParser
import datetime

import bs4
from cassandra.cluster import Cluster
import numpy as np
import pandas as pd
import requests


class ExchangeRateParsingError(Exception):
    pass


def fx_rates_url(start, end, base, *symbols):
    symbols = [symbol.upper() for symbol in [*symbols]]
    symbols = ','.join(symbols)
    return 'https://api.exchangeratesapi.io/history?start_at={start}&end_at={end}&symbols={symbols}&base={base}'.format(
        start=start, end=end, symbols=symbols, base=base)


def historical_coin_url(slug, start, end):
    return 'https://coinmarketcap.com/currencies/{slug}/historical-data/?start={start}&end={end}'.format(
        slug=slug, start=start.replace('-', ''), end=end.replace('-', ''))


def all_url():
    return 'https://coinmarketcap.com/all/views/all/'


def parse_all_response(resp):
    soup = bs4.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table')
    columns = ['slug'] + [x.get('id', 'th-#')[3:] for x in table.thead.find_all('th')]

    def get_val(td):
        tag = td
        # some columns like price store values within inner <a>
        if tag.find('a'):
            tag = tag.find('a')
        # numeric columns store their value in these attributes in addition to text.
        # use these attributes to avoid parsing $ and , in text
        for key in ['data-usd', 'data-supply']:
            val = tag.get(key)
            if val:
                try:
                    return np.float64(val)
                except ValueError:
                    return np.nan
        return tag.text

    rows = []
    for tr in table.tbody.find_all('tr'):
        slug = tr.get('id')[3:]  # remove 'id-' prefix from id
        rows.append([slug] + [get_val(x) for x in tr.find_all('td')])

    return pd.DataFrame(columns=columns, data=rows)


def parse_historical_coin_response(resp):
    soup = bs4.BeautifulSoup(resp.text, 'lxml')
    soup_hist = soup.find(id='historical-data')
    if not soup_hist:
        return

    table = soup_hist.find('table')
    # they added *'s to the end of some columns
    columns = [x.text.lower().replace(' ', '').rstrip('*') for x in table.thead.find_all('th')]

    def get_val(td):
        # numeric columns store their value in this attribute in addition to text
        val = td.get('data-format-value')
        if val:
            try:
                return np.float64(val)
            except ValueError:
                return np.nan
        return td.text

    rows = []
    for tr in table.tbody.find_all('tr'):
        if tr.td.text == 'No data was found for the selected time period.':
            return
        rows.append([get_val(x) for x in tr.find_all('td')])

    df = pd.DataFrame(columns=columns, data=rows)
    df['date'] = pd.to_datetime(df.date).astype('str')
    return df


def parse_fx_rates_response(resp):
    fx_data = resp.json()
    df = pd.DataFrame.from_dict(fx_data['rates'], orient='index') .reset_index()
    df.columns = ['date', 'fx_rate']
    return df


def lookup_slug(all_df, symbol):
    if not symbol.isupper():
        symbol = symbol.upper()
    df_row = all_df.loc[all_df['symbol'] == symbol]
    if df_row.empty:
        return None
    slug = df_row['slug'].tolist()
    if(len(slug) > 1):
        raise ExchangeRateException("Found more than one possible slugs")
    return slug[0]


def query_required_currencies(session, keyspace, table):
    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    session.row_factory = pandas_factory

    query = """SELECT column_name FROM system_schema.columns
               WHERE
                   keyspace_name = '{keyspace}' AND
                   table_name = '{fx_table_name}';""".format(keyspace=keyspace,
                                                       fx_table_name=table)
    result = session.execute(query)
    df = result._current_rows
    currencies = [symbol.upper() for symbol in list(df['column_name'])]
    if 'USD' not in currencies or 'DATE' not in currencies:
        raise ExchangeRateParsingError("USD is mandatory and must be defined in the schema.")
    currencies.remove('DATE')
    currencies.remove('USD')
    return currencies


def query_most_recent_date(session):
    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)
    session.row_factory = pandas_factory

    query = """SELECT date from exchange_rates;"""

    result = session.execute(query)
    df = result._current_rows
    if len(df) == 0:
        return None
    df['date'] = df['date'].astype('datetime64')

    largest = df.nlargest(1, 'date').iloc[0]['date']

    return largest.strftime("%Y-%m-%d")


def fetch_crypto_exchange_rates(start, end, crypto_currency):
    all_crypto_df = parse_all_response(requests.get(all_url()))
    slug = lookup_slug(all_crypto_df, crypto_currency)
    crypto_url = historical_coin_url(slug, start, end)
    print("Fetching {} exchange rates from {}".format(crypto_currency, crypto_url))
    crypto_resp = requests.get(crypto_url)
    crypto_df = parse_historical_coin_response(crypto_resp)
    crypto_df = crypto_df[['date','close']].rename(columns={'close':'USD'})
    return crypto_df


def insert_exchange_rates(session, exchange_rates_df):
    colnames = ','.join(exchange_rates_df.columns)
    values = ','.join(['?' for i in range(len(exchange_rates_df.columns))])
    query = """INSERT INTO exchange_rates({})
               VALUES ({})""".format(colnames, values)
    prepared = session.prepare(query)

    for index, row in exchange_rates_df.iterrows():
        session.execute(prepared, row)


def main():
    parser = ArgumentParser(description='Ingest exchange rates into Cassandra',
                            epilog='GraphSense - http://graphsense.info')
    parser.add_argument('-d', '--db_nodes', dest='db_nodes', nargs='+',
                        default=['localhost'], metavar='DB_NODE',
                        help='list of Cassandra nodes; default "localhost")')
    parser.add_argument('-k', '--keyspace', dest='keyspace',
                        required=True,
                        help='Cassandra keyspace')
    parser.add_argument('-t', '--table', dest='table',
                        default='exchange_rates',
                        help='Name of the target echange rate table')
    parser.add_argument('--start_date', dest='start',
                        type=str, default='2009-01-01',
                        help='start date for fetching exchange rates')
    parser.add_argument('--end_date', dest='end',
                        type=str, default=datetime.date.today()
                                                       .strftime("%Y-%m-%d"),
                        help='end date for fetching exchange rates')
    parser.add_argument('-c', '--cryptocurrency', dest='cryptocurrency',
                        type=str, default='BTC',
                        required=True,
                        help='the target cryptocurrency')

    args = parser.parse_args()

    cluster = Cluster(args.db_nodes)
    keyspace = args.keyspace
    table = args.table

    session = cluster.connect(keyspace)

    # Default start and end date
    start = args.start
    end = args.end

    crypto_currency = args.cryptocurrency

    print("*** Starting exchange rate ingest for {} ***"
          .format(crypto_currency))

    # Query most recent data in 'exchange_rates' table
    try:
        most_recent_date = query_most_recent_date(session)
        if most_recent_date is not None:
            start = most_recent_date
        print("Start date: {}".format(start))
        print("End date: {}".format(end))
    except ExchangeRateParsingError as err:
        print("Error while querying most recent date: {}".format(err))

    # Query all required fiat currencies from the 'exchange_rates' table
    try:
        fiat_currencies = query_required_currencies(session, keyspace, table)
        print("Target fiat currencies: {}".format(fiat_currencies))
    except ExchangeRateParsingError as err:
        print("Error while querying all required fiat currencies: {}".format(err))

    # Fetch crypto currency exchange rates in USD
    try:
        crypto_df = fetch_crypto_exchange_rates(start, end, crypto_currency)
    except ExchangeRateParsingError as err:
        print("Error while fetching exchange rates in USD: {}".format(err))

    # Query conversion rates and merge converted values in exchange rates
    try:
        exchange_rates = crypto_df
        for fiat_currency in fiat_currencies:
            fx_url = fx_rates_url(start, end, "USD", fiat_currency)
            print("Fetching conversion rates for {} from {}".format(fiat_currency, fx_url))
            fx_resp = requests.get(fx_url)
            fx_df = parse_fx_rates_response(fx_resp)
            merged_df = crypto_df.merge(fx_df, how="left", on='date')
            merged_df['fx_rate'] = merged_df['fx_rate'].interpolate(method='linear')
            merged_df['fx_rate'] = merged_df['fx_rate'].fillna(method='ffill')
            merged_df['fx_rate'] = merged_df['fx_rate'].fillna(method='bfill')    
            merged_df[fiat_currency] = merged_df['USD'] * merged_df['fx_rate']
            merged_df = merged_df[['date', fiat_currency]]
            exchange_rates = exchange_rates.merge(merged_df, on='date')
    except ExchangeRateParsingError as err:
        print("Error while querying conversion rates: {}".format(err))

    # Insert final exchange rates into Cassandra 'exchange rates' table
    try:
        print("Inserted rates for {} days".format(len(exchange_rates)))
        insert_exchange_rates(session, exchange_rates)
    except ExchangeRateParsingError as err:
        print("Error while inserting exchange rates into Cassandra: {}".format(err))

    cluster.shutdown()


if __name__ == '__main__':
    main()








