import google.cloud.exceptions
import pandas
import google_pandas_load
import utils


class LoaderInitErrorTest(utils.base_class.BaseClassTest):
    def test_raise_error_if_dataset_id_none_bq_client_not_none(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(
                bq_client=utils.constants.bq_client, dataset_id=None)
        msg = 'dataset_id must be provided if bq_client is provided'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_bq_client_none_dataset_id_not_none(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(bq_client=None, dataset_id='a.b')
        msg = 'bq_client must be provided if dataset_id is provided'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_bucket_name_none_gs_client_not_none(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(
                gs_client=utils.constants.gs_client, bucket_name=None)
        msg = 'bucket_name must be provided if gs_client is provided'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_gs_client_none_bucket_not_none(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(gs_client=None, bucket_name='bn')
        msg = 'gs_client must be provided if bucket_name is provided'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_dataset_id_not_contain_exactly_one_dot(self):
        msg = 'dataset_id must contain exactly one dot'

        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(dataset_id='ab')
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(dataset_id='a.b.c')
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_bucket_dir_path_is_empty_string(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(bucket_dir_path='')
        msg = 'bucket_dir_path must not be the empty string'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_bucket_dir_path_starts_with_slash(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(bucket_dir_path='/dir/subdir')
        msg = 'bucket_dir_path must not start with /'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_bucket_dir_path_ends_with_slash(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(bucket_dir_path='dir/subdir/')
        msg = 'bucket_dir_path must not end with /'
        self.assertEqual(msg, str(cm.exception))


class LoaderQuickSetupInitTest(utils.base_class.BaseClassTest):
    def test_raise_error_if_d_and_b_none_project_id_not_none(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader_quick_setup(
                project_id='pi', dataset_name=None, bucket_name=None)
        msg = ('At least one of dataset_name or bucket_name '
               'must be provided if project_id is provided')
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_project_id_none_dataset_name_not_none(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader_quick_setup(project_id=None)
        msg = 'project_id must be provided if dataset_name is provided'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_project_id_none_bucket_name_not_none(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader_quick_setup(
                project_id=None, dataset_name=None)
        msg = 'project_id must be provided if bucket_name is provided'
        self.assertEqual(msg, str(cm.exception))


class LoadConfigErrorTest(utils.base_class.BaseClassTest):

    def test_raise_error_if_data_name_is_empty_string(self):
        with self.assertRaises(ValueError) as cm:
            google_pandas_load.LoadConfig(
                source='query', destination='dataset',
                query='select 3', data_name='')
        msg = 'data_name must not be the empty string'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_data_name_contains_slash(self):
        with self.assertRaises(ValueError) as cm:
            google_pandas_load.LoadConfig(
                source='query', destination='dataset',
                query='select 3', data_name='a/b')
        msg = 'data_name=a/b must not contain a /'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_invalid_source(self):
        with self.assertRaises(ValueError) as cm:
            google_pandas_load.LoadConfig(
                source='queryy', destination='dataframe', query='select 3')
        msg = ("source must be one of 'query' or 'dataset' or "
               "'bucket' or 'local' or 'dataframe")
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_invalid_destination(self):
        df = pandas.DataFrame(data={'x': [1]})
        with self.assertRaises(ValueError) as cm:
            google_pandas_load.LoadConfig(
                source='dataframe', destination='query', dataframe=df)
        msg = ("destination must be one of 'dataset' or 'bucket' or 'local' "
               "or 'dataframe'")
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_source_is_equal_to_destination(self):
        with self.assertRaises(ValueError) as cm:
            google_pandas_load.LoadConfig(
                source='local', destination='local', data_name='a')
        msg = 'source must be different from destination'
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_missing_required_values(self):
        with self.assertRaises(ValueError) as cm:
            google_pandas_load.LoadConfig(
                source='query', destination='bucket', query='select 3')
        msg = ("data_name must be provided if source or destination is one of "
               "'dataset' or 'bucket' or 'local'")
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            google_pandas_load.LoadConfig(
                source='query', destination='dataframe')
        msg = "query must be provided if source = 'query'"
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            google_pandas_load.LoadConfig(
                source='dataframe', destination='local', data_name='a1')
        msg = "dataframe must be provided if source = 'dataframe'"
        self.assertEqual(msg, str(cm.exception))

    def test_raise_error_if_infer_bq_schema_from_no_columns_dataframe(self):
        with self.assertRaises(ValueError) as cm:
            google_pandas_load.LoadConfig.bq_schema_inferred_from_dataframe(
                dataframe=pandas.DataFrame(data={}))
        msg = ('A non empty bq_schema cannot be inferred '
               'from a dataframe with no columns')
        self.assertEqual(str(cm.exception), msg)


class ListErrorTest(utils.base_class.BaseClassTest):

    def test_raise_error_if_data_name_contains_slash(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader().list_blobs(data_name='a/b')
        msg = 'data_name=a/b must not contain a /'
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader().list_blob_uris(data_name='a/b')
        msg = 'data_name=a/b must not contain a /'
        self.assertEqual(msg, str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader().list_local_file_paths(data_name='a/b')
        msg = 'data_name=a/b must not contain a /'
        self.assertEqual(msg, str(cm.exception))


class LoadErrorTest(utils.base_class.BaseClassTest):

    def test_raise_error_if_configs_is_not_a_list(self):
        config = google_pandas_load.LoadConfig(
            source='bucket', destination='local', data_name='a1')
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader().multi_load(configs={config})
        self.assertEqual('configs must be a list', str(cm.exception))

    def test_raise_error_if_configs_is_empty(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader().multi_load(configs=[])
        self.assertEqual('configs must be non-empty', str(cm.exception))

    def test_raise_error_if_prefix(self):
        config1 = google_pandas_load.LoadConfig(
            source='dataframe',
            destination='dataset',
            dataframe=pandas.DataFrame(data={'x': [3]}),
            data_name='a')
        config2 = google_pandas_load.LoadConfig(
            source='query',
            destination='dataframe',
            query='select 4 as y',
            data_name='aa')
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader().multi_load(configs=[config1, config2])
        self.assertEqual('a is a prefix of aa', str(cm.exception))

    def test_raise_error_if_missing_required_resources(self):

        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(
                bq_client=None, dataset_id=None).load(
                source='query', destination='dataset',
                data_name='e0', query='select 3')
        self.assertEqual('bq_client must be provided if dataset is used',
                         str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(
                gs_client=None, bucket_name=None).load(
                source='bucket', destination='local', data_name='a')
        self.assertEqual('gs_client must be provided if bucket is used',
                         str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader(local_dir_path=None).load(
                source='dataframe', destination='local',
                dataframe=pandas.DataFrame(data={'x': [1]}), data_name='a')
        self.assertEqual('local_dir_path must be provided if local is used',
                         str(cm.exception))

    def test_raise_error_if_no_data(self):
        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader().load(
                source='dataset', destination='local', data_name='e0')
        self.assertEqual('There is no data named e0 in dataset',
                         str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader().load(
                source='bucket', destination='dataset', data_name='e0')
        self.assertEqual('There is no data named e0 in bucket',
                         str(cm.exception))

        with self.assertRaises(ValueError) as cm:
            utils.loader.create_loader().load(
                source='local', destination='dataframe', data_name='e0')
        self.assertEqual('There is no data named e0 in local',
                         str(cm.exception))

    def test_raise_error_if_syntax_error_in_query(self):
        with self.assertRaises(google.cloud.exceptions.BadRequest):
            utils.loader.create_loader().load(
                source='query', destination='dataset',
                query='selectt 3', data_name='a3')

    def test_raise_error_if_write_empty_and_already_exists(self):
        utils.populate.populate_dataset()
        utils.populate.populate_local()
        with self.assertRaises(google.cloud.exceptions.Conflict) as cm:
            utils.loader.create_loader().load(
                source='local',
                destination='dataset',
                data_name='a10',
                write_disposition='WRITE_EMPTY')
        self.assertEqual(
            str(cm.exception),
            '409 Already Exists: Table dmp-y-tests:test_gpl.a10')
