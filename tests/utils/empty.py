from tests import utils


def empty_dataset():
    for n in utils.list.list_table_names():
        utils.delete.delete_table(n)


def empty_bucket():
    blobs = utils.list.list_blobs()
    utils.constants.bucket.delete_blobs(blobs=blobs)
