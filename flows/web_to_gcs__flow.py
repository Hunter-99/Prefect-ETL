import pandas as pd
from prefect import (
    flow,
    task
)

from prefect_gcp.cloud_storage import GcsBucket
from settings import BASE_DIR


@flow(log_prints=True)
def web_to_gcs(color: str = "green", year: int = 2020, month: int = 1) -> None:
    dataset_file_name = f"{color}_tripdata_{year}-{month:02}"
    dataset_file_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/" \
                       f"{color}/{dataset_file_name}.csv.gz"

    raw_dataset = extract_data(
        dataset_file_url=dataset_file_url
    )
    dataset = transform_data(
        raw_dataset=raw_dataset
    )
    load_data(
        dataset=dataset,
        dataset_folder=f"{color}_taxi",
        dataset_file_name=dataset_file_name
    )


@task()
def extract_data(dataset_file_url: str) -> pd.DataFrame():
    raw_dataset = pd.read_csv(
        filepath_or_buffer=dataset_file_url
    )
    print(f"Detected {len(raw_dataset)} records in dataset")

    return raw_dataset


@task()
def transform_data(raw_dataset: pd.DataFrame) -> pd.DataFrame:
    if 'lpep_pickup_datetime' in raw_dataset.columns:
        raw_dataset['lpep_pickup_datetime'] = pd.to_datetime(raw_dataset['lpep_pickup_datetime'])
    if 'lpep_dropoff_datetime' in raw_dataset.columns:
        raw_dataset['lpep_dropoff_datetime'] = pd.to_datetime(raw_dataset['lpep_dropoff_datetime'])
    if 'tpep_pickup_datetime' in raw_dataset.columns:
        raw_dataset['tpep_pickup_datetime'] = pd.to_datetime(raw_dataset['tpep_pickup_datetime'])
    if 'tpep_dropoff_datetime' in raw_dataset.columns:
        raw_dataset['tpep_dropoff_datetime'] = pd.to_datetime(raw_dataset['tpep_dropoff_datetime'])
    raw_dataset['passenger_count'] = raw_dataset['passenger_count'].fillna(0)

    return raw_dataset


@task()
def load_data(dataset: pd.DataFrame, dataset_folder: str, dataset_file_name: str) -> None:
    dataset_filepath = f"{BASE_DIR}/datasets/{dataset_folder}/{dataset_file_name}.parquet"

    dataset.to_parquet(
        dataset_filepath,
        compression='gzip'
    )

    gcs_bucket_block = GcsBucket.load('gcs-bucket', validate=False)
    gcs_bucket_block.upload_from_path(
        from_path=dataset_filepath,
        to_path=f"{dataset_folder}/{dataset_file_name}.parquet"
    )


if __name__ == '__main__':
    web_to_gcs()
