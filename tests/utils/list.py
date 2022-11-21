from tests import utils


def list_table_names():
    return [t.table_id for t in utils.constants.bq_client.list_tables(
        utils.constants.dataset_id)]


def list_blobs():
    return list(utils.constants.bucket.list_blobs())
