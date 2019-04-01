from google_pandas_load import LoadConfig
from tests.context.loaders import *
from tests.utils import *


class LoadConfigRaiseErrorTest(unittest.TestCase):

    def test_raise_error_if_invalid_source_or_destination(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='queryy', destination='dataframe')
        self.assertEqual(str(cm.exception), "source must be one of 'query' or 'bq' or 'gs' or 'local' or 'dataframe'")

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='dataframee')
        self.assertEqual(
            str(cm.exception), "destination must be one of 'query' or 'bq' or 'gs' or 'local' or 'dataframe'")

    def test_raise_error_if_source_is_equal_to_destination(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='query')
        self.assertEqual(str(cm.exception), 'source must be different from destination')

    def test_raise_error_if_missing_required_values(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='dataframe')
        self.assertEqual(str(cm.exception), "query must be given if source == 'query'")

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='dataframe', destination='local')
        self.assertEqual(str(cm.exception), "dataframe must be given if source == 'dataframe'")

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='query', destination='gs', query='select 3')
        self.assertEqual(
            str(cm.exception), "data_name must be given if source or destination is one of 'bq' or 'gs' or 'local'")

        with self.assertRaises(ValueError) as cm:
            LoadConfig(source='gs', destination='bq', data_name='e0')
        self.assertEqual(str(cm.exception), 'bq_schema is missing')

    def test_raise_error_if_bq_schema_inferred_from_dataframe_is_given_a_dataframe_with_no_columns(self):
        with self.assertRaises(ValueError) as cm:
            LoadConfig.bq_schema_inferred_from_dataframe(dataframe=pandas.DataFrame(data={}))
        self.assertEqual(str(cm.exception), 'A non empty bq_schema cannot be inferred from a dataframe with no columns')


class LoaderSetupRaiseErrorTest(BaseClassTest):

    def test_raise_error_if_gs_dir_path_ends_with_slash(self):
        with self.assertRaises(ValueError) as cm:
            Loader(gs_dir_path_in_bucket='dir/subdir/')
        self.assertEqual(str(cm.exception),
                         'To simplify Storage path concatenation, gs_dir_path_in_bucket must not end with /')


class LoadRaiseErrorTest(BaseClassTest):

    def test_raise_error_if_prefix(self):
        config1 = LoadConfig(
            source='dataframe',
            destination='query',
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
            gpl1.load(source='bq', destination='query', data_name='e0')
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
        self.assertEqual(str(cm.exception), 'There is no data named e0 in local')
        with self.assertRaises(ValueError) as cm:
            gpl1.load(source='local', destination='gs', data_name='e0')
        self.assertEqual(str(cm.exception), 'There is no data named e0 in local')

    def test_raise_error_if_missing_required_resources(self):
        with self.assertRaises(ValueError) as cm:
            gpl_no_bq_client.load(source='query', destination='bq', query='select 3', data_name='e0')
        self.assertEqual(str(cm.exception), 'bq_client must be given if bq is used')

        with self.assertRaises(ValueError) as cm:
            gpl_no_dataset_ref.load(source='query', destination='bq', query='select 3', data_name='e0')
        self.assertEqual(str(cm.exception), 'dataset_ref must be given if bq is used')

        with self.assertRaises(ValueError) as cm:
            gpl_no_bucket.load(source='gs', destination='local', data_name='e0')
        self.assertEqual(str(cm.exception), 'bucket must be given if gs is used')

        with self.assertRaises(ValueError) as cm:
            gpl_no_local_dir_path.load(source='local', destination='gs', data_name='e0')
        self.assertEqual(str(cm.exception), 'local_dir_path must be given if local is used')
