import os
import shutil
import json
import unittest
from google_pandas_load.utils import wait_for_jobs
from tests.context.resources import *


def clean():
    # empty the dataset
    tables = bq_client.list_tables(dataset_ref)
    for t in tables:
        bq_client.delete_table(table=t.reference)

    # empty the bucket
    blobs = bucket.list_blobs()
    bucket.delete_blobs(blobs=blobs)

    # delete the local folder
    if os.path.isdir(local_dir_path):
        shutil.rmtree(local_dir_path)


class BaseClassTest(unittest.TestCase):
    def setUp(self):
        clean()
        os.makedirs(local_dir_path)

    def tearDown(self):
        clean()


def populate_dataset(table_ids):
    jobs = []
    for n in table_ids:
        table_ref = dataset_ref.table(n)
        job_config = bigquery.QueryJobConfig()
        job_config.destination = table_ref
        job = bq_client.query(query="select 'bq_{}' as x".format(n), job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs=jobs)


def populate_bucket(blob_names):
    for n in blob_names:
        storage.Blob(name=n, bucket=bucket).upload_from_string(data='gs_{}'.format(n))


def populate_local_folder(local_file_basenames):
    for n in local_file_basenames:
        with open(os.path.join(local_dir_path, 'a{}'.format(i)), 'w') as outfile:
            json.dump('data', outfile)


def populate():
    populate_dataset()
    populate_bucket()
    populate_local_folder()

