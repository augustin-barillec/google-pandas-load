from tests.utils.empty import empty_dataset, empty_bucket
from tests.utils.delete import delete_local_dir


def clean():
    empty_dataset()
    empty_bucket()
    delete_local_dir()
