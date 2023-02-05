from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../data/")
    return Path(f"../data/{gcs_path}")

# @task()
# def transform(path: Path) -> pd.DataFrame:
#     """Data cleaning example"""
#     df = pd.read_parquet(path)
#     print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
#     df["passenger_count"].fillna(0, inplace=True)
#     print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
#     return df


@task(log_prints=True)
def print_processed_rows(df: pd.DataFrame) -> pd.DataFrame:
    print(f"rows: {len(df)}")
    return len(df)

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="utility-logic-375619",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_parent_flow(months: list[int] = [1, 2], year: int = 2021, color: str = "yellow"):
    """
    The main ETL function
    The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

    Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

    Make any other necessary changes to the code for it to function as required.

    Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

    """
    rows = 0
    for month in months:
        #etl_web_to_gcs(year, month, color)
        path = extract_from_gcs(color, year, month)
        df = pd.read_parquet(path)
        current_rows = print_processed_rows(df)
        rows = rows + current_rows
        write_bq(df)

    print(rows)


# set parameters in flow
if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    etl_parent_flow(months, year, color)


