import os
import pandas
from google.cloud import bigquery, storage
from google_pandas_load.utils import wait_for_jobs
from tests.resources import bq_client, dataset_id, bucket, gs_dir_path, \
    gs_subdir_path, local_dir_path, local_subdir_path

table_names_default = tuple(f'a{i}_bq' for i in range(8, 12))
blob_basenames_0_default = tuple(f'a{i}_gs0' for i in range(9, 12))
blob_basenames_1_default = tuple(f'a{i}_gs1' for i in range(8, 11))
blob_basenames_2_default = tuple(f'a{i}_gs2' for i in range(7, 13))
local_file_basenames_0_default = tuple(f'a{i}_local0' for i in range(7, 15))
local_file_basenames_1_default = tuple(f'a{i}_local1' for i in range(9, 14))


def populate_bq(table_names=table_names_default):
    jobs = []
    for n in table_names:
        table_id = f'{dataset_id}.{n}'
        job_config = bigquery.QueryJobConfig()
        job_config.destination = table_id
        job = bq_client.query(
            query=f"select 'data_{n}' as x",
            job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs)


def _populate_gs_folder(gs_folder_path, blob_basenames):
    for n in blob_basenames:
        if gs_folder_path is None:
            blob_name = n
        else:
            blob_name = f'{gs_folder_path}/{n}'
        storage.Blob(name=blob_name, bucket=bucket).upload_from_string(
            data=f'x\ndata_{n}')


def populate_gs(
        blob_basenames_0=blob_basenames_0_default,
        blob_basenames_1=blob_basenames_1_default,
        blob_basenames_2=blob_basenames_2_default):
    _populate_gs_folder(None, blob_basenames_0)
    _populate_gs_folder(gs_dir_path, blob_basenames_1)
    _populate_gs_folder(gs_subdir_path, blob_basenames_2)


def _populate_local_folder(local_folder_path, local_file_basenames):
    for n in local_file_basenames:
        df = pandas.DataFrame(data={'x': [f'data_{n}']})
        file_path = os.path.join(local_folder_path, n)
        df.to_csv(path_or_buf=file_path, index=False)


def populate_local(
        local_file_basenames_0=local_file_basenames_0_default,
        local_file_basenames_1=local_file_basenames_1_default):
    _populate_local_folder(local_dir_path, local_file_basenames_0)
    _populate_local_folder(local_subdir_path, local_file_basenames_1)


def populate():
    populate_bq()
    populate_gs()
    populate_local()
