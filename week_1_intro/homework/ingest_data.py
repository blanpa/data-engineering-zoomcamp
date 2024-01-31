#!/usr/bin/env python
# coding: utf-8
import argparse
import os
from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    port = params.port
    host = params.host
    db = params.db
    table_name_zones = params.table_name_zones
    table_name_trips = params.table_name_trips
    url_zones = "https://raw.githubusercontent.com/atsterq/data-engineering-zoomcamp-2024/main/01-docker-terraform/homework/taxi%2B_zone_lookup.csv"
    url_trips = "https://raw.githubusercontent.com/atsterq/data-engineering-zoomcamp-2024/main/01-docker-terraform/homework/green_tripdata_2019-09.csv"
    csv_zones = "zones.csv"
    csv_trips = "trips.csv"

    # download data
    os.system(f"wget {url_zones} -O {csv_zones}")
    os.system(f"wget {url_trips} -O {csv_trips}")

    # connect to database
    engine = create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{db}"
    )

    # upload zones to database
    df1 = pd.read_csv(csv_zones)
    df1.to_sql(name=table_name_zones, con=engine, if_exists="append")

    # upload trips to database and change columns type
    df_iter = pd.read_csv(csv_trips, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name_trips, con=engine, if_exists="replace")

    while True:
        t_start = time()

        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name=table_name_trips, con=engine, if_exists="append")

        t_end = time()

        print(
            "sucsessfully inserted another chunk... it took %.3f second"
            % (t_end - t_start)
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest CSV data to postgres."
    )
    parser.add_argument("--user", help="username for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--db", help="database name for postgres")
    parser.add_argument(
        "--table_name_zones",
        help="name of the table where we will write the result to",
    )
    parser.add_argument(
        "--table_name_trips",
        help="name of the table where we will write the result to",
    )

    args = parser.parse_args()

    main(args)
