import pandas
from io import StringIO
from google.cloud import bigquery
from google.cloud import storage
from tests.utils.resources import bq_client, bucket, separator
from tests.utils.ids import build_table_id, build_gs_uri


def wait_for_jobs(jobs):
    for job in jobs:
        job.result()


def multi_query_to_bq(queries, table_names):
    jobs = []
    for q, n in zip(queries, table_names):
        job_config = bigquery.QueryJobConfig()
        job_config.destination = build_table_id(n)
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job = bq_client.query(query=q, job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs)


def multi_bq_to_gs(table_names, blob_names):
    jobs = []
    for tn, bn in zip(table_names, blob_names):
        source = build_table_id(tn)
        job_config = bigquery.ExtractJobConfig()
        destination_uri = build_gs_uri(bn)
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job_config.field_delimiter = separator
        job = bq_client.extract_table(
            source=source,
            destination_uris=destination_uri,
            job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs)


def multi_gs_to_bq(blob_names, table_names):
    jobs = []
    for bn, tn in zip(blob_names, table_names):
        job_config = bigquery.LoadJobConfig()
        job_config.field_delimiter = separator
        job_config.write_disposition = 'WRITE_TRUNCATE'
        source_uri = build_gs_uri(bn)
        destination = build_table_id(tn)
        job = bq_client.load_table_from_uri(
            source_uris=source_uri,
            destination=destination,
            job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs)


def gs_to_local(blob_name, local_file_path):
    storage.Blob(name=blob_name, bucket=bucket).download_to_filename(
        local_file_path)


def local_to_gs(local_file_path, blob_name):
    storage.Blob(name=blob_name, bucket=bucket).upload_from_filename(
        local_file_path)


def local_to_dataframe(local_file_path):
    return pandas.read_csv(local_file_path, sep=separator)


def dataframe_to_local(df, local_file_path):
    return df.to_csv(local_file_path, sep=separator, index=False)


def query_to_dataframe(query):
    return bq_client.query(query).to_dataframe()


def bq_to_dataframe(table_name):
    table_id = build_table_id(table_name)
    return bq_client.list_rows(table_id).to_dataframe()


def multi_dataframe_to_bq(dfs, table_names):
    jobs = []
    for df, n in zip(dfs, table_names):
        table_id = build_table_id(n)
        job_config = bigquery.LoadJobConfig()
        job = bq_client.load_table_from_dataframe(
            dataframe=df,
            destination=table_id,
            job_config=job_config)
        jobs.append(job)
    wait_for_jobs(jobs)


def dataframe_to_gs(df, blob_name):
    df_csv = df.to_csv(sep=separator, index=False)
    storage.Blob(name=blob_name, bucket=bucket).upload_from_string(df_csv)


def gs_to_dataframe(blob_name):
    df_csv = storage.Blob(name=blob_name, bucket=bucket).download_as_text()
    return pandas.read_csv(StringIO(df_csv))
