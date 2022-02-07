import pandas
from google.cloud.exceptions import BadRequest, Conflict
from google_pandas_load import Loader, LoadConfig
from tests.utils.resources import bq_client, gs_client
from tests.utils.populate import populate_bq, populate_local
from tests.utils import loaders
from tests.utils.base_class import BaseClassTest


class LoadConfigErrorTest(BaseClassTest):

    def test_raise_error_if_data_name_is_empty_string(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(
                source='query', destination='bq',
                query='select 3', data_name='')
        msg = 'data_name must not be the empty string'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_data_name_contains_slash(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(
                source='query', destination='bq',
                query='select 3', data_name='a/b')
        msg = 'data_name=a/b must not contain a /'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_invalid_source(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='queryy', destination='dataframe',
                       query='select 3')
        msg = ("source must be one of 'query' or 'bq' or 'gs' or 'local' "
               "or 'dataframe")
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_invalid_destination(self):
        df = pandas.DataFrame(data={'x': [1]})
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='dataframe', destination='query',
                       dataframe=df)
        msg = ("destination must be one of 'bq' or 'gs' or 'local' "
               "or 'dataframe'")
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_source_is_equal_to_destination(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='local', destination='local',
                       data_name='a')
        msg = 'source must be different from destination'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_missing_required_values(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='gs', query='select 3')
        msg = ("data_name must be given if source or destination is one of "
               "'bq' or 'gs' or 'local'")
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='dataframe')
        msg = "query must be given if source = 'query'"
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='dataframe', destination='local', data_name='a1')
        msg = "dataframe must be given if source = 'dataframe'"
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_infer_bq_schema_from_no_columns_dataframe(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig.bq_schema_inferred_from_dataframe(
                dataframe=pandas.DataFrame(data={}))
        msg = ('A non empty bq_schema cannot be inferred '
               'from a dataframe with no columns')
        self.assertEqual(str(cm.exception), msg)


class LoaderSetupErrorTest(BaseClassTest):

    def test_raise_error_if_dataset_id_none_bq_client_not_none(self):
        with self.assertRaises(ValueError) as cm:
            Loader(bq_client=bq_client, dataset_id=None)
        msg = 'dataset_id must not be None if bq_client is not None'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_bq_client_none_dataset_id_not_none(self):
        with self.assertRaises(ValueError) as cm:
            Loader(bq_client=None, dataset_id='di')
        msg = 'bq_client must not be None if dataset_id is not None'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_bucket_name_none_gs_client_not_none(self):
        with self.assertRaises(ValueError) as cm:
            Loader(gs_client=gs_client, bucket_name=None)
        msg = 'bucket_name must not be None if gs_client is not None'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_gs_client_none_bucket_not_none(self):
        with self.assertRaises(ValueError) as cm:
            Loader(gs_client=None, bucket_name='bn')
        msg = 'gs_client must not be None if bucket_name is not None'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_dataset_id_not_contain_exactly_one_dot(self):
        msg = 'dataset_id must contain exactly one dot'

        with self.assertRaises(ValueError) as cm:
            Loader(dataset_id='ab')
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            Loader(dataset_id='a.b.c')
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_gs_dir_path_is_empty_string(self):
        with self.assertRaises(ValueError) as cm:
            Loader(gs_dir_path='')
        msg = 'gs_dir_path must not be the empty string'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_gs_dir_path_starts_with_slash(self):
        with self.assertRaises(ValueError) as cm:
            Loader(gs_dir_path='/dir/subdir')
        msg = 'gs_dir_path must not start with /'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_gs_dir_path_ends_with_slash(self):
        with self.assertRaises(ValueError) as cm:
            Loader(gs_dir_path='dir/subdir/')
        msg = 'gs_dir_path must not end with /'
        self.assertEqual(msg, str(cm.exception))


class ListErrorTest(BaseClassTest):

    def test_raise_error_if_data_name_contains_slash(self):
        with self.assertRaises(ValueError) as cm:
            Loader().list_blobs(data_name='a/b')
        msg = 'data_name=a/b must not contain a /'
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            Loader().list_blob_uris(data_name='a/b')
        msg = 'data_name=a/b must not contain a /'
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            Loader().list_local_file_paths(data_name='a/b')
        msg = 'data_name=a/b must not contain a /'
        self.assertEqual(msg, str(cm.exception))


class LoadErrorTest(BaseClassTest):

    def test_raise_error_if_configs_is_not_a_list(self):
        config = LoadConfig(
            source='gs', destination='local', data_name='a1')
        with self.assertRaises(ValueError) as cm:
            loaders.gpl21.mload(configs={config})
        self.assertEqual('configs must be a list', str(cm.exception))

    def test_raise_error_if_configs_is_empty(self):
        with self.assertRaises(ValueError) as cm:
            loaders.gpl00.mload(configs=[])
        self.assertEqual('configs must be non-empty', str(cm.exception))

    def test_raise_error_if_prefix(self):
        config1 = LoadConfig(
            source='dataframe',
            destination='bq',
            dataframe=pandas.DataFrame(data={'x': [3]}),
            data_name='a')
        config2 = LoadConfig(
            source='query',
            destination='dataframe',
            query='select 4 as y',
            data_name='aa')
        with self.assertRaises(ValueError) as cm:
            loaders.gpl01.mload(configs=[config1, config2])
        self.assertEqual('a is a prefix of aa', str(cm.exception))

    def test_raise_error_if_missing_required_resources(self):

        with self.assertRaises(ValueError) as cm:
            Loader(bq_client=None).load(
                source='query', destination='bq',
                data_name='e0', query='select 3')
        self.assertEqual('bq_client must be given if bq is used',
                         str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            Loader(gs_client=None).load(
                source='gs', destination='local', data_name='a')
        self.assertEqual('gs_client must be given if gs is used',
                         str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            Loader(local_dir_path=None).load(
                source='dataframe', destination='local',
                dataframe=pandas.DataFrame(data={'x': [1]}), data_name='a')
        self.assertEqual('local_dir_path must be given if local is used',
                         str(cm.exception))

    def test_raise_error_if_no_data(self):
        with self.assertRaises(ValueError) as cm:
            loaders.gpl00.load(
                source='bq', destination='local', data_name='e0')
        self.assertEqual('There is no data named e0 in bq',
                         str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            loaders.gpl00.load(
                source='gs', destination='bq', data_name='e0')
        self.assertEqual('There is no data named e0 in gs',
                         str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            loaders.gpl00.load(
                source='local', destination='dataframe', data_name='e0')
        self.assertEqual('There is no data named e0 in local',
                         str(cm.exception))

    def test_raise_error_if_syntax_error_in_query(self):
        with self.assertRaises(BadRequest):
            loaders.gpl20.load(
                source='query', destination='bq',
                query='selectt 3', data_name='a3')

    def test_raise_error_if_write_empty_and_already_exists(self):
        populate_bq()
        populate_local()
        with self.assertRaises(Conflict) as cm:
            loaders.gpl01.load(
                source='local',
                destination='bq',
                data_name='a10',
                write_disposition='WRITE_EMPTY')
        self.assertEqual(
            str(cm.exception),
            '409 Already Exists: Table dmp-y-tests:test_gpl.a10')
