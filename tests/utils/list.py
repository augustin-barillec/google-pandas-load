from tests.utils.resources import bq_client, dataset_id, bucket


def list_table_names():
    return [t.table_id for t in bq_client.list_tables(dataset_id)]


def list_blobs():
    return list(bucket.list_blobs())
