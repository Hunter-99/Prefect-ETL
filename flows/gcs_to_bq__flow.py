import pandas as pd

from prefect import (
    flow,
    task
)

from settings import BASE_DIR
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@flow(log_prints=True)
def gcs_to_bq(color="green", year=2020, month=1):
    """The main ETL function. Inserts data from """
    dataset_folder = f"{color}_taxi"
    dataset_file_name = f"{color}_tripdata_{year}-{month:02}.parquet"

    local_dataset_filepath = extract_data_from_gcs(
        dataset_folder=dataset_folder,
        dataset_file_name=dataset_file_name
    )

    dataset = transform_data(
        local_dataset_filepath=local_dataset_filepath
    )

    load_data(
        dataset_schema=dataset_folder,
        dataset=dataset
    )


@task()
def extract_data_from_gcs(dataset_folder: str, dataset_file_name: str) -> str:
    """Download data from GCS"""

    gcs_dataset_filepath = f"{dataset_folder}/{dataset_file_name}"
    local_dataset_filepath = f"{BASE_DIR}/datasets/{dataset_folder}/{dataset_file_name}"

    gcs_bucket_block = GcsBucket.load('gcs-bucket')
    gcs_bucket_block.get_directory(
        from_path=gcs_dataset_filepath,
        local_path=local_dataset_filepath
    )

    print(f"File {gcs_dataset_filepath} successfully downloaded to {local_dataset_filepath}!")

    return local_dataset_filepath


@task()
def transform_data(local_dataset_filepath: str) -> pd.DataFrame:
    dataset = pd.read_parquet(
        path=local_dataset_filepath
    )

    print(f"Missing passenger count before transformation: {dataset['passenger_count'].isna().sum()}")
    dataset['passenger_count'] = dataset['passenger_count'].fillna(0)
    print(f"Missing passenger count after transformation: {dataset['passenger_count'].isna().sum()}")

    return dataset


@task()
def load_data(dataset_schema: str, dataset: pd.DataFrame) -> None:
    gcp_credentials_block = GcpCredentials.load('gcp-credentials')
    dataset.to_gbq(
        destination_table=f"trips_data_all.{dataset_schema}",
        project_id='chrome-encoder-375816',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=100_000,
        if_exists='append'
    )


if __name__ == '__main__':
    gcs_to_bq()
