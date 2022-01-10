from tests.utils.resources import bucket
from tests.utils.list import list_table_names, list_blobs
from tests.utils.delete import delete_table


def empty_dataset():
    for n in list_table_names():
        delete_table(n)


def empty_bucket():
    blobs = list_blobs()
    bucket.delete_blobs(blobs=blobs)
