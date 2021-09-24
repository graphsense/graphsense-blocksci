#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Script to fetch cryptocurrency exchange rates from CoinMarketCap."""

from argparse import ArgumentParser
from datetime import date, datetime, timedelta
import json
from typing import List, Optional

from cassandra.cluster import Cluster, Session
import pandas as pd
import requests

MIN_START = "2009-01-01"
FX_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.zip"


def fetch_ecb_rates(symbol_list: List) -> pd.DataFrame:
    """Fetch and preprocess FX rates from ECB."""

    print(f"Fetching conversion rates for FIAT currencies:\n{FX_URL}")
    rates_eur = pd.read_csv(FX_URL)  # exchange rates based on EUR
    rates_eur = rates_eur.iloc[:, :-1]  # remove empty last column
    rates_eur["EUR"] = 1.0
    # convert to values based on USD
    rates_usd = rates_eur[symbol_list].div(rates_eur.USD, axis=0)
    rates_usd["date"] = rates_eur.Date
    print(f"Last record: {rates_usd.date.tolist()[0]}")
    return rates_usd


def cmc_historical_url(symbol: str, start: date, end: date) -> str:
    """Returns URL for CoinMarketCap API request."""
    return (
        "https://web-api.coinmarketcap.com/v1/cryptocurrency/ohlcv/"
        + f"historical?symbol={symbol}&convert=USD"
        + f"&time_start={start}&time_end={end}"
    )


def parse_cmc_historical_response(
    response: requests.Response,
) -> pd.DataFrame:
    """Parse historical exchange rates (JSON) from CoinMarketCap."""

    json_data = json.loads(response.content)
    json_data = [
        [elem["time_close"][:10], elem["quote"]["USD"]["close"]]
        for elem in json_data["data"]["quotes"]
    ]

    return pd.DataFrame(json_data, columns=["date", "USD"])


def query_most_recent_date(
    session: Session, keyspace: str, table: str
) -> Optional[str]:
    """Fetch most recent entry from exchange rates table."""

    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

    session.row_factory = pandas_factory

    query = f"""SELECT date FROM {keyspace}.{table};"""

    result = session.execute(query)
    rates = result._current_rows

    if rates.empty:
        max_date = None
    else:
        rates["date"] = rates["date"].astype("datetime64")
        max_date = (
            rates.nlargest(1, "date").iloc[0]["date"].strftime("%Y-%m-%d")
        )
    return max_date


def fetch_cmc_rates(
    start: str, end: str, crypto_currency: str
) -> pd.DataFrame:
    """Fetch cryptocurrency exchange rates from CoinMarketCap."""

    user_agent = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 11_1) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/87.0.4280.88 Safari/537.36"
    )
    headers = {"User-Agent": user_agent}

    start_date = date.fromisoformat(start) + timedelta(days=-1)
    end_date = date.fromisoformat(end)
    url = cmc_historical_url(crypto_currency, start_date, end_date)

    print(f"Fetching {crypto_currency} exchange rates:\n{url}")
    response = requests.get(url, headers=headers)
    cmc_rates = parse_cmc_historical_response(response)

    print(f"Last record: {cmc_rates.date.tolist()[-1]}")
    return cmc_rates


def insert_exchange_rates(
    session: Session, keyspace: str, table: str, exchange_rates: pd.DataFrame
) -> None:
    """Insert exchange rates into Cassandra table."""

    colnames = ",".join(exchange_rates.columns)
    values = ",".join(["?" for i in range(len(exchange_rates.columns))])
    query = f"""INSERT INTO {keyspace}.{table}({colnames}) VALUES ({values})"""
    prepared = session.prepare(query)

    for _, row in exchange_rates.iterrows():
        session.execute(prepared, row)


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
    parser.add_argument(
        "--cryptocurrency",
        dest="cryptocurrency",
        type=str,
        default="BTC",
        required=True,
        help="target cryptocurrency",
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
        start_date = MIN_START

    # query most recent data
    if not args.force:
        most_recent_date = query_most_recent_date(
            session, args.keyspace, args.table
        )
        if most_recent_date is not None:
            start_date = most_recent_date

    print(f"*** Starting exchange rate ingest for {args.cryptocurrency} ***")
    print(f"Start date: {start_date}")
    print(f"End date: {end_date}")
    print(f"Target fiat currencies: {args.fiat_currencies}")

    if datetime.fromisoformat(start_date) > datetime.fromisoformat(end_date):
        print("Error: start date after end date.")
        cluster.shutdown()
        raise SystemExit

    # fetch cryptocurrency exchange rates in USD
    cmc_rates = fetch_cmc_rates(start_date, end_date, args.cryptocurrency)

    ecb_rates = fetch_ecb_rates(args.fiat_currencies)
    # query conversion rates and merge converted values in exchange rates
    exchange_rates = cmc_rates
    date_range = pd.date_range(
        date.fromisoformat(start_date), date.fromisoformat(end_date)
    )
    date_range = pd.DataFrame(date_range, columns=["date"])
    date_range = date_range["date"].dt.strftime("%Y-%m-%d")

    for fiat_currency in set(args.fiat_currencies) - set(["USD"]):
        ecb_rate = ecb_rates[["date", fiat_currency]].rename(
            columns={fiat_currency: "fx_rate"}
        )
        merged_df = cmc_rates.merge(ecb_rate, on="date", how="left").merge(
            date_range, how="right"
        )
        # fill gaps over weekends
        merged_df["fx_rate"].fillna(method="ffill", inplace=True)
        merged_df["fx_rate"].fillna(method="bfill", inplace=True)
        merged_df[fiat_currency] = merged_df["USD"] * merged_df["fx_rate"]
        merged_df = merged_df[["date", fiat_currency]]
        exchange_rates = exchange_rates.merge(merged_df, on="date")

    # insert final exchange rates into Cassandra
    if "USD" not in args.fiat_currencies:
        exchange_rates.drop("USD", axis=1, inplace=True)
    exchange_rates["fiat_values"] = exchange_rates.drop(
        "date", axis=1
    ).to_dict(orient="records")
    exchange_rates.drop(args.fiat_currencies, axis=1, inplace=True)

    print(f"{exchange_rates.iloc[0].date} - {exchange_rates.iloc[-1].date}")

    # insert exchange rates into Cassandra table
    insert_exchange_rates(session, args.keyspace, args.table, exchange_rates)
    print(f"Inserted rates for {len(exchange_rates)} days: ", end="")
    print(f"{exchange_rates.iloc[0].date} - {exchange_rates.iloc[-1].date}")

    cluster.shutdown()


if __name__ == "__main__":
    main()
