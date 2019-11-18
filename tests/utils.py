import os
import shutil
import unittest
import pandas
from google_pandas_load.utils import wait_for_jobs
from tests.context.resources import *


def empty_dataset():
    tables = bq_client.list_tables(dataset_ref)
    for t in tables:
        bq_client.delete_table(table=t.reference)


def empty_bucket():
    blobs = bucket.list_blobs()
    bucket.delete_blobs(blobs=blobs)


def delete_local_folder():
    if os.path.isdir(local_dir_path):
        shutil.rmtree(local_dir_path)


def clean():
    empty_dataset()
    empty_bucket()
    delete_local_folder()


class BaseClassTest(unittest.TestCase):
    def setUp(self):
        clean()
        os.makedirs(local_dir_path)

    def tearDown(self):
        pass
        # clean()


table_ids_default = tuple('a{}_bq'.format(i) for i in range(8, 12))
blob_names_root_default = tuple('a{}_gs'.format(i) for i in range(9, 12))
blob_names_deep_default = tuple('dir/subdir/a{}_gs'.format(i) for i in range(7, 11))
local_files_basenames_default = tuple('a{}_local'.format(i) for i in range(9, 14))


def populate_dataset(table_ids=table_ids_default):
    jobs = []
    for n in table_ids:
        table_ref = dataset_ref.table(n)
        job_config = bigquery.QueryJobConfig()
        job_config.destination = table_ref
        job = bq_client.query(query="select 'data_{}' as x".format(n), job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs=jobs)


def populate_bucket(blob_names=blob_names_root_default + blob_names_deep_default):
    for n in blob_names:
        storage.Blob(name=n, bucket=bucket).upload_from_string(data='data_{}'.format(n))


def populate_local_folder(local_file_basenames=local_files_basenames_default):
    for n in local_file_basenames:
        df = pandas.DataFrame(data={'x': ['data_{}'.format(n)]})
        file_path = os.path.join(local_dir_path, n)
        df.to_csv(file_path, sep='|', index=False)


def populate():
    populate_dataset()
    populate_bucket()
    populate_local_folder()

