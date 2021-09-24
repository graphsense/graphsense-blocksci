#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Script to fetch cryptocurrency exchange rates from CoinDesk."""

from argparse import ArgumentParser
from datetime import date, datetime, timedelta

from cassandra.cluster import Cluster
import pandas as pd
import requests


def query_most_recent_date(session, keyspace, table):
    """Fetch most recent entry from exchange rates table.

    Parameters
    ----------
    session
        Cassandra session.
    keyspace
        Target Cassandra keyspace.
    table
        Cassandra table.

    Returns
    -------
    DataFrame
        Exchange rates in pandas DataFrame with columns 'date', 'USD', 'EUR'.
    """

    def pandas_factory(colnames, rows):
        """Cassandra row factory for pandas DataFrames."""
        return pd.DataFrame(rows, columns=colnames)

    session.row_factory = pandas_factory

    query = f"""SELECT date FROM {keyspace}.{table};"""

    result = session.execute(query)
    exchange_rates = result._current_rows
    if exchange_rates.empty:
        return None
    exchange_rates["date"] = exchange_rates["date"].astype("datetime64")

    largest = exchange_rates.nlargest(1, "date").iloc[0]["date"]

    return largest.strftime("%Y-%m-%d")


def fetch_exchange_rates(start, end, symbol_list):
    """Fetch BTC exchange rates from CoinDesk.

    Parameters
    ----------
    start : str
        Start date (ISO-format YYYY-mm-dd).
    end : str
        End date (ISO-format YYYY-mm-dd).
    symbol_list: list[str]
        ["EUR", "USD", "JPY" ...]

    Returns
    -------
    DataFrame
        Exchange rates in pandas DataFrame with columns 'date', 'fiat_values'
    """

    base_url = "https://api.coindesk.com/v1/bpi/historical/close.json"
    param = "?currency={}&start={}&end={}"

    df_merged = pd.DataFrame()

    for fiat in symbol_list:
        new_request = requests.get(base_url + param.format(fiat, start, end))
        try:
            new_json = new_request.json()
            print(new_json["disclaimer"])
            new_df = pd.DataFrame.from_records([new_json["bpi"]]).transpose()
            new_df.rename(columns={0: fiat}, inplace=True)

            df_merged = new_df.join(df_merged)
        except:
            print(f"Unknown currency: {fiat}")

    df_merged.reset_index(level=0, inplace=True)
    df_merged.rename(columns={"index": "date"}, inplace=True)
    df_merged["fiat_values"] = df_merged.drop("date", axis=1).to_dict(
        orient="records"
    )

    [
        df_merged.drop(c, axis=1, inplace=True)
        for c in symbol_list
        if c in df_merged.keys()
    ]
    return df_merged


def insert_exchange_rates(session, keyspace, table, exchange_rates):
    """Insert exchange rates into Cassandra table.

    Parameters
    ----------
    session
        Cassandra session.
    keyspace
        Target Cassandra keyspace.
    table
        Cassandra table.
    exchange_rates
        pandas DataFrame with columns 'date', 'USD', 'EUR' etc.
    """

    colnames = ",".join(exchange_rates.columns)
    values = ",".join(["?" for i in range(len(exchange_rates.columns))])
    query = f"""INSERT INTO {keyspace}.{table}({colnames}) VALUES ({values})"""
    prepared = session.prepare(query)

    for _, row in exchange_rates.iterrows():
        session.execute(prepared, row)


def main():
    """Main function."""

    MIN_START = "2010-10-17"  # no CoinDesk exchange rates available before
    prev_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    parser = ArgumentParser(
        description="Ingest exchange rates into Cassandra",
        epilog="GraphSense - http://graphsense.info",
    )
    parser.add_argument(
        "-d",
        "--db_nodes",
        dest="db_nodes",
        nargs="+",
        default=["localhost"],
        metavar="DB_NODE",
        help="list of Cassandra nodes; default 'localhost'",
    )
    parser.add_argument(
        "-f",
        "--force",
        dest="force",
        action="store_true",
        help="do not fetch most recent entries from "
        "Cassandra and overwrite existing records",
    )
    parser.add_argument(
        "--fiat_currencies",
        dest="fiat_currencies",
        nargs="+",
        default=["USD", "EUR"],
        metavar="FIAT_CURRENCY",
        help="list of fiat currencies; default ['USD', 'EUR']",
    )
    parser.add_argument(
        "-k",
        "--keyspace",
        dest="keyspace",
        required=True,
        help="Cassandra keyspace",
    )
    parser.add_argument(
        "-t",
        "--table",
        dest="table",
        default="exchange_rates",
        help="name of the target exchange rate table",
    )
    parser.add_argument(
        "--start_date",
        dest="start",
        type=str,
        default=MIN_START,
        help="start date for fetching exchange rates",
    )
    parser.add_argument(
        "--end_date",
        dest="end",
        type=str,
        default=prev_date,
        help="end date for fetching exchange rates",
    )

    args = parser.parse_args()

    cluster = Cluster(args.db_nodes)
    keyspace = args.keyspace
    table = args.table
    session = cluster.connect(keyspace)

    # default start and end date
    start = args.start
    end = args.end

    print("*** Starting exchange rate ingest for BTC ***")

    if datetime.fromisoformat(start) < datetime.fromisoformat(MIN_START):
        print(f"Warning: Exchange rates not available before {MIN_START}")
        start = MIN_START

    # query most recent data in 'exchange_rates' table
    if not args.force:
        most_recent_date = query_most_recent_date(session, keyspace, table)
        if most_recent_date is not None:
            start = most_recent_date

    print(f"Start date: {start}")
    print(f"End date: {end}")

    if datetime.fromisoformat(start) > datetime.fromisoformat(end):
        print("Error: start date after end date.")
        cluster.shutdown()
        raise SystemExit

    print(f"Target fiat currencies: {args.fiat_currencies}")
    exchange_rates_df = fetch_exchange_rates(start, end, args.fiat_currencies)

    # insert exchange rates into Cassandra table
    print(f"Inserted rates for {len(exchange_rates_df)} days")
    insert_exchange_rates(session, keyspace, table, exchange_rates_df)

    cluster.shutdown()


if __name__ == "__main__":
    main()
