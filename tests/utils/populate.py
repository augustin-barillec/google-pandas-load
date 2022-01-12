import pandas
from tests.utils.load import multi_query_to_bq, dataframe_to_gs, \
    dataframe_to_local
from tests.utils import ids

table_names = tuple(f'a{i}' for i in range(7, 12))
blob_basenames_0 = tuple(f'a{i}' for i in range(7, 12))
blob_basenames_1 = tuple(f'a{i}' for i in range(8, 13))
blob_basenames_2 = tuple(f'a{i}' for i in range(9, 14))
local_file_basenames_0 = tuple(f'a{i}' for i in range(7, 12))
local_file_basenames_1 = tuple(f'a{i}' for i in range(8, 13))

blob_names_0 = [ids.build_blob_name_0(n) for n in blob_basenames_0]
blob_names_1 = [ids.build_blob_name_1(n) for n in blob_basenames_1]
blob_names_2 = [ids.build_blob_name_2(n) for n in blob_basenames_2]

local_file_paths_0 = [
    ids.build_local_file_path_0(n) for n in local_file_basenames_0]
local_file_paths_1 = [
    ids.build_local_file_path_1(n) for n in local_file_basenames_1]

blob_basenames = blob_basenames_0 + blob_basenames_1 + blob_basenames_2
local_file_basenames = local_file_basenames_0 + local_file_basenames_1
blob_names = blob_names_0 + blob_names_1 + blob_names_2
local_file_paths = local_file_paths_0 + local_file_paths_1


def populate_bq():
    queries = [f"select '{n}' as x" for n in table_names]
    multi_query_to_bq(queries, table_names)


def populate_gs():
    for basename, name in zip(blob_basenames, blob_names):
        df = pandas.DataFrame(data={'x': [f'{basename}']})
        dataframe_to_gs(df, name)


def populate_local():
    for b, p in zip(local_file_basenames, local_file_paths):
        df = pandas.DataFrame(data={'x': [f'{b}']})
        dataframe_to_local(df, p)


def populate():
    populate_bq()
    populate_gs()
    populate_local()
