from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect_gcp import GcpCredentials
from prefect.tasks import task_input_hash
import requests

### Functions for web to GCS


@task()
def write_gcs(path: Path) -> None:
    """Uploading local parquet file to google cloud storage"""
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path)

    return


### Subflows here
@flow()
def etl_web_to_gcs(year: int, month: int) -> None:
    """gets the data from web and stores it into gcs"""

    dataset_file = f"green_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/{dataset_file}.csv.gz"

    path = Path(f"data/green/{dataset_file}.csv.gz")
    output_path = Path(f"data/green/{dataset_file}.parquet")

    output_dir = Path(f"data/green")
    output_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(dataset_url, parse_dates=["lpep_pickup_datetime", "lpep_dropoff_datetime"])

    df.to_parquet(output_path, compression="gzip")

    write_gcs(output_path)


# Main flow here
@flow(log_prints=True)
def etl_parent_flow(months: list[int] = [1, 2, 3], year: int = 2021):
    for month in months:
        print(f"Doing the ETL for {year}-{month}")
        etl_web_to_gcs(year, month)


if __name__ == "__main__":
    months = [i for i in range(1, 13)]
    year = 2019

    etl_parent_flow(months, year)
