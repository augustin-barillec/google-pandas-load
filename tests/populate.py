import os
import pandas
from google.cloud import bigquery, storage
from google_pandas_load.utils import wait_for_jobs
from tests.context.resources import bq_client, dataset_ref, bucket, \
    local_dir_path

table_names_default = tuple(f'a{i}_bq' for i in range(8, 12))
blob_names_root_default = tuple(f'a{i}_gs' for i in range(9, 12))
blob_names_semi_deep_default = tuple(f'dir/a{i}_gs' for i in range(8, 11))
blob_names_deep_default = tuple(f'dir/subdir/a{i}_gs' for i in range(7, 11))
local_files_basenames_default = tuple(f'a{i}_local' for i in range(9, 14))


def populate_dataset(table_names=table_names_default):
    jobs = []
    for n in table_names:
        table_ref = dataset_ref.table(n)
        job_config = bigquery.QueryJobConfig()
        job_config.destination = table_ref
        job = bq_client.query(
            query=f"select 'data_{n}' as x",
            job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs)


def populate_bucket(
        blob_names=
        blob_names_root_default +
        blob_names_semi_deep_default +
        blob_names_deep_default):
    for n in blob_names:
        storage.Blob(name=n, bucket=bucket).upload_from_string(
            data=f'data_{n}')


def populate_local_folder(local_file_basenames=local_files_basenames_default):
    for n in local_file_basenames:
        df = pandas.DataFrame(data={'x': [f'data_{n}']})
        file_path = os.path.join(local_dir_path, n)
        df.to_csv(path_or_buf=file_path, sep='|', index=False)


def populate():
    populate_dataset()
    populate_bucket()
    populate_local_folder()
