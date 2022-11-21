import io
import zlib
import pandas
from google.cloud import bigquery, storage
from tests import utils


def wait_for_jobs(jobs):
    for job in jobs:
        job.result()


def multi_query_to_dataset(queries, table_names):
    jobs = []
    for q, n in zip(queries, table_names):
        job_config = bigquery.QueryJobConfig()
        job_config.destination = utils.ids.build_table_id(n)
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job = utils.constants.bq_client.query(query=q, job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs)


def bucket_to_local(blob_name, local_file_path):
    storage.Blob(
        name=blob_name, bucket=utils.constants.bucket).download_to_filename(
        local_file_path)


def local_to_dataframe(local_file_path):
    return pandas.read_csv(local_file_path, sep=utils.constants.separator)


def dataframe_to_local(dataframe, local_file_path):
    return dataframe.to_csv(
        local_file_path, sep=utils.constants.separator, index=False)


def dataset_to_dataframe(table_name):
    table_id = utils.ids.build_table_id(table_name)
    return utils.constants.bq_client.list_rows(table_id).to_dataframe()


def multi_dataframe_to_dataset(dataframes, table_names):
    jobs = []
    for df, n in zip(dataframes, table_names):
        table_id = utils.ids.build_table_id(n)
        job_config = bigquery.LoadJobConfig()
        job = utils.constants.bq_client.load_table_from_dataframe(
            dataframe=df,
            destination=table_id,
            job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs)


def dataframe_to_bucket(dataframe, blob_name):
    csv = dataframe.to_csv(sep=utils.constants.separator, index=False)
    storage.Blob(
        name=blob_name, bucket=utils.constants.bucket).upload_from_string(csv)


def bucket_to_dataframe(blob_name, decompress):
    b = storage.Blob(
        name=blob_name, bucket=utils.constants.bucket).download_as_bytes()
    if decompress:
        b = zlib.decompress(b, wbits=zlib.MAX_WBITS | 16)
    csv = b.decode('utf-8')
    return pandas.read_csv(
        io.StringIO(csv), sep=utils.constants.separator)
