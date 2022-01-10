import pandas
from tests.utils.load import multi_query_to_bq, dataframe_to_gs, \
    dataframe_to_local
from tests.utils import ids

table_names = tuple(f'a{i}_bq' for i in range(8, 12))
blob_basenames_0 = tuple(f'a{i}_gs0' for i in range(9, 12))
blob_basenames_1 = tuple(f'a{i}_gs1' for i in range(8, 11))
blob_basenames_2 = tuple(f'a{i}_gs2' for i in range(7, 13))
local_file_basenames_0 = tuple(f'a{i}_local0' for i in range(7, 15))
local_file_basenames_1 = tuple(f'a{i}_local1' for i in range(9, 14))

blob_names_0 = [ids.build_blob_name_0(n) for n in blob_basenames_0]
blob_names_1 = [ids.build_blob_name_1(n) for n in blob_basenames_1]
blob_names_2 = [ids.build_blob_name_2(n) for n in blob_basenames_2]

local_file_paths_0 = [
    ids.build_local_file_path_0(n) for n in local_file_basenames_0]
local_file_paths_1 = [
    ids.build_local_file_path_1(n) for n in local_file_basenames_1]

blob_names = blob_names_0 + blob_names_1 + blob_names_2
local_file_paths = local_file_paths_0 + local_file_paths_1


def populate_bq():
    queries = [f"select 'data_{n}' as x" for n in table_names]
    multi_query_to_bq(queries, table_names)


def populate_gs():
    for n in blob_names:
        df = pandas.DataFrame(data={'x': [f'data_{n}']})
        dataframe_to_gs(df, blob_names)


def populate_local():
    for p in local_file_paths:
        df = pandas.DataFrame(data={'x': [f'data_{p}']})
        dataframe_to_local(df, p)


def populate():
    populate_bq()
    populate_gs()
    populate_local()
