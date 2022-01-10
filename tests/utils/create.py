import os
from tests.utils.resources import local_subdir_path


def create_local_subdir():
    os.makedirs(local_subdir_path)
