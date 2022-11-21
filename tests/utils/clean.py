from tests import utils


def clean():
    utils.empty.empty_dataset()
    utils.empty.empty_bucket()
    utils.delete.delete_local_dir()
