import os
from tests import utils


def create_local_subdir():
    os.makedirs(utils.constants.local_subdir_path)
