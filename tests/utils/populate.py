import pandas
from tests import utils

table_names = tuple(f'a{i}' for i in range(7, 12))
blob_basenames_0 = tuple(f'a{i}' for i in range(7, 12))
blob_basenames_1 = tuple(f'a{i}' for i in range(8, 13))
blob_basenames_2 = tuple(f'a{i}' for i in range(9, 14))
local_file_basenames_0 = tuple(f'a{i}' for i in range(7, 12))
local_file_basenames_1 = tuple(f'a{i}' for i in range(8, 13))

blob_names_0 = [utils.ids.build_blob_name_0(n) for n in blob_basenames_0]
blob_names_1 = [utils.ids.build_blob_name_1(n) for n in blob_basenames_1]
blob_names_2 = [utils.ids.build_blob_name_2(n) for n in blob_basenames_2]

local_file_paths_0 = [
    utils.ids.build_local_file_path_0(n) for n in local_file_basenames_0]
local_file_paths_1 = [
    utils.ids.build_local_file_path_1(n) for n in local_file_basenames_1]

blob_basenames = blob_basenames_0 + blob_basenames_1 + blob_basenames_2
local_file_basenames = local_file_basenames_0 + local_file_basenames_1
blob_names = blob_names_0 + blob_names_1 + blob_names_2
local_file_paths = local_file_paths_0 + local_file_paths_1


def populate_dataset():
    queries = [f"select '{n}_dataset' as x" for n in table_names]
    utils.load.multi_query_to_dataset(queries, table_names)


def populate_bucket():
    for basename, name in zip(blob_basenames, blob_names):
        df = pandas.DataFrame(data={'x': [f'{basename}_bucket']})
        utils.load.dataframe_to_bucket(df, name)


def populate_local():
    for b, p in zip(local_file_basenames, local_file_paths):
        df = pandas.DataFrame(data={'x': [f'{b}_local']})
        utils.load.dataframe_to_local(df, p)


def populate():
    populate_dataset()
    populate_bucket()
    populate_local()
