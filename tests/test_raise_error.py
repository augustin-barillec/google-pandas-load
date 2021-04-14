import pandas
from google.cloud import bigquery
from google.cloud.exceptions import BadRequest
from google_pandas_load import Loader, LoadConfig
from tests.context.loaders import gpl1, gpl2, gpl5, gpl_no_bq_client, \
    gpl_no_dataset_ref, gpl_no_bucket, gpl_no_local_dir_path
from tests.base_class import BaseClassTest
from tests.populate import populate


class UtilsRaiseErrorTest(BaseClassTest):

    def test_wait_for_jobs_runtime_error(self):
        with self.assertRaises(BadRequest):
            gpl1.load(source='query', destination='bq', data_name='a3',
                      query='selectt 3')


class LoadConfigRaiseErrorTest(BaseClassTest):

    def test_raise_error_if_invalid_source(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='queryy', destination='dataframe',
                       query='select 3')
        msg = ("source must be one of 'query' or 'bq' or 'gs' or 'local' "
               "or 'dataframe")
        self.assertEqual(str(cm.exception), msg)

    def test_raise_error_if_invalid_destination(self):
        df = pandas.DataFrame(data={'x': [1]})
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='dataframe', destination='query',
                       dataframe=df)
        msg = ("destination must be one of 'bq' or 'gs' or 'local' "
               "or 'dataframe'")
        self.assertEqual(str(cm.exception), msg)

    def test_raise_error_if_source_is_equal_to_destination(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='local', destination='local',
                       data_name='a')
        self.assertEqual(str(cm.exception),
                         'source must be different from destination')

    def test_raise_error_if_missing_required_values(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='gs', query='select 3')
        msg = ("data_name must be given if source or destination is one of "
               "'bq' or 'gs' or 'local'")
        self.assertEqual(str(cm.exception), msg)

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='dataframe')
        self.assertEqual(str(cm.exception),
                         "query must be given if source = 'query'")

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='dataframe', destination='local', data_name='a1')
        self.assertEqual(str(cm.exception),
                         "dataframe must be given if source = 'dataframe'")

    def test_raise_error_if_infer_bq_schema_from_no_columns_dataframe(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig.bq_schema_inferred_from_dataframe(
                dataframe=pandas.DataFrame(data={}))
        msg = ('A non empty bq_schema cannot be inferred '
               'from a dataframe with no columns')
        self.assertEqual(str(cm.exception), msg)


class LoaderSetupRaiseErrorTest(BaseClassTest):

    def test_raise_error_if_gs_dir_path_ends_with_slash(self):
        with self.assertRaises(ValueError) as cm:
            Loader(gs_dir_path='dir/subdir/')
        msg = ("To ease Storage path concatenation, gs_dir_path must"
               " not end with /")
        self.assertEqual(str(cm.exception), msg)


class LoadRaiseErrorTest(BaseClassTest):

    def test_raise_error_if_configs_is_not_a_list(self):
        config = LoadConfig(
            source='gs', destination='local', data_name='a1')
        with self.assertRaises(ValueError) as cm:
            gpl5.mload(configs={config})
        self.assertEqual(str(cm.exception), 'configs must be list')

    def test_raise_error_if_configs_is_empty(self):
        with self.assertRaises(ValueError) as cm:
            gpl5.mload(configs=[])
        self.assertEqual(str(cm.exception), 'configs must be non-empty')

    def test_raise_error_if_prefix(self):
        config1 = LoadConfig(
            source='dataframe',
            destination='bq',
            data_name='a',
            dataframe=pandas.DataFrame(data={'x': [3]}))
        config2 = LoadConfig(
            source='query',
            destination='dataframe',
            data_name='aa',
            query='select 4 as y')
        with self.assertRaises(ValueError) as cm:
            gpl2.mload(configs=[config1, config2])
        self.assertEqual(str(cm.exception), 'a is a prefix of aa')

    def test_raise_error_if_no_data(self):
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='bq', destination='local', data_name='e0')
        self.assertEqual(str(cm.exception), 'There is no data named e0 in bq')
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='bq', destination='gs', data_name='e0')
        self.assertEqual(str(cm.exception), 'There is no data named e0 in bq')

        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='gs', destination='local', data_name='e0')
        self.assertEqual(str(cm.exception), 'There is no data named e0 in gs')
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='gs', destination='bq', data_name='e0',
                      bq_schema=[bigquery.SchemaField('x', 'INTEGER')])
        self.assertEqual(str(cm.exception), 'There is no data named e0 in gs')

        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='local', destination='dataframe', data_name='e0')
        self.assertEqual(str(cm.exception),
                         'There is no data named e0 in local')
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='local', destination='gs', data_name='e0')
        self.assertEqual(str(cm.exception),
                         'There is no data named e0 in local')

    def test_raise_error_if_missing_required_resources(self):
        populate()

        with self.assertRaises(ValueError) as cm:
            gpl_no_bq_client.load(source='query', destination='bq',
                                  query='select 3', data_name='e0')
        self.assertEqual(str(cm.exception),
                         'bq_client must be given if bq is used')

        with self.assertRaises(ValueError) as cm:
            gpl_no_dataset_ref.load(source='query', destination='bq',
                                    query='select 3', data_name='e0')
        self.assertEqual(str(cm.exception),
                         'dataset_ref must be given if bq is used')

        with self.assertRaises(ValueError) as cm:
            gpl_no_bucket.load(source='gs', destination='local',
                               data_name='a')
        self.assertEqual(str(cm.exception),
                         'bucket must be given if gs is used')

        with self.assertRaises(ValueError) as cm:
            gpl_no_local_dir_path.load(source='local', destination='gs',
                                       data_name='a')
        self.assertEqual(str(cm.exception),
                         'local_dir_path must be given if local is used')
