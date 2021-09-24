#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Script to fetch cryptocurrency exchange rates from CoinDesk."""

from argparse import ArgumentParser
from datetime import date, datetime, timedelta
from typing import List, Optional

from cassandra.cluster import Cluster, Session
import pandas as pd
import requests
from simplejson.errors import JSONDecodeError


MIN_START = "2010-10-17"  # no CoinDesk exchange rates available before


def query_most_recent_date(
    session: Session, keyspace: str, table: str
) -> Optional[str]:
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
    str
        Most recent date in Exchange rates in Cassadra table.
    """

    def pandas_factory(colnames, rows):
        """Cassandra row factory for pandas DataFrames."""
        return pd.DataFrame(rows, columns=colnames)

    session.row_factory = pandas_factory

    query = f"""SELECT date FROM {keyspace}.{table};"""

    result = session.execute(query)
    rates = result._current_rows
    rates["date"] = rates["date"].astype("datetime64")

    if rates.empty:
        max_date = None
    else:
        rates["date"] = rates["date"].astype("datetime64")
        max_date = (
            rates.nlargest(1, "date").iloc[0]["date"].strftime("%Y-%m-%d")
        )
    return max_date


def fetch_exchange_rates(
    start_date: str, end_date: str, symbol_list: List
) -> pd.DataFrame:
    """Fetch BTC exchange rates from CoinDesk.

    Parameters
    ----------
    start_date : str
        Start date (ISO-format YYYY-mm-dd).
    end_date : str
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
        request = requests.get(
            base_url + param.format(fiat, start_date, end_date)
        )
        try:
            json = request.json()
            print(json["disclaimer"])
            rates = pd.DataFrame.from_records([json["bpi"]]).transpose()
            rates.rename(columns={0: fiat}, inplace=True)
            df_merged = rates.join(df_merged)
        except JSONDecodeError as symbol_not_found:
            print(f"Unknown currency: {fiat}")
            raise SystemExit(1) from symbol_not_found

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


def insert_exchange_rates(
    session: Session, keyspace: str, table: str, exchange_rates: pd.DataFrame
) -> None:
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
    prepared_stmt = session.prepare(query)

    for _, row in exchange_rates.iterrows():
        session.execute(prepared_stmt, row)


def create_parser() -> ArgumentParser:
    """Create command-line argument parser."""

    prev_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    parser = ArgumentParser(
        description="Ingest exchange rates into Cassandra",
        epilog="GraphSense - http://graphsense.info",
    )
    parser.add_argument(
        "-d",
        "--db-nodes",
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
        help="don't continue from last found Cassandra record "
        "and force overwrite of existing rows",
    )
    parser.add_argument(
        "--fiat-currencies",
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
        "--start-date",
        dest="start_date",
        type=str,
        default=MIN_START,
        help="start date for fetching exchange rates",
    )
    parser.add_argument(
        "--end-date",
        dest="end_date",
        type=str,
        default=prev_date,
        help="end date for fetching exchange rates",
    )
    return parser


def main() -> None:
    """Main function."""

    args = create_parser().parse_args()

    cluster = Cluster(args.db_nodes)
    session = cluster.connect(args.keyspace)

    # default start and end date
    start_date = args.start_date
    end_date = args.end_date

    if datetime.fromisoformat(start_date) < datetime.fromisoformat(MIN_START):
        print(f"Warning: Exchange rates not available before {MIN_START}")
        start_date = MIN_START

    # query most recent data in 'exchange_rates' table
    if not args.force:
        most_recent_date = query_most_recent_date(
            session, args.keyspace, args.table
        )
        if most_recent_date is not None:
            start_date = most_recent_date

    print("*** Starting exchange rate ingest for BTC ***")
    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")

    if datetime.fromisoformat(start_date) > datetime.fromisoformat(end_date):
        print("Error: start date after end date.")
        cluster.shutdown()
        raise SystemExit(1)

    print(f"Target fiat currencies: {args.fiat_currencies}")
    exchange_rates = fetch_exchange_rates(
        start_date, end_date, args.fiat_currencies
    )

    # insert exchange rates into Cassandra table
    insert_exchange_rates(session, args.keyspace, args.table, exchange_rates)
    print(f"Inserted rates for {len(exchange_rates)} days: ", end="")
    print(f"{exchange_rates.iloc[0].date} - {exchange_rates.iloc[-1].date}")

    cluster.shutdown()


if __name__ == "__main__":
    main()
