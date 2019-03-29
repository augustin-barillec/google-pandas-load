import os
import json
import pandas
from tests.context.loaders import *
from tests.base_class_for_unit_tests import BaseClassTest


class DeleteMethodsTest(BaseClassTest):

    def test_delete_in_bq(self):
        df = pandas.DataFrame(data={'x': [1]})
        table_ref = dataset_ref.table('a0')
        bq_client.load_table_from_dataframe(dataframe=df, destination=table_ref).result()
        self.assertTrue(gpl4.exist_in_bq(data_name='a0'))
        gpl4.delete_in_bq(data_name='a0')
        self.assertFalse(gpl4.exist_in_bq(data_name='a0'))

    def test_delete_in_gs(self):
        storage.Blob(name='a0', bucket=bucket).upload_from_string('data')
        self.assertTrue(gpl1.exist_in_gs(data_name='a0'))
        gpl1.delete_in_gs(data_name='a0')
        self.assertFalse(gpl1.exist_in_gs(data_name='a0'))

        storage.Blob(name='dir/subdir/a0', bucket=bucket).upload_from_string('data')
        self.assertTrue(gpl2.exist_in_gs(data_name='a0'))
        gpl2.delete_in_gs(data_name='a0')
        self.assertFalse(gpl2.exist_in_gs(data_name='a0'))

    def test_delete_in_local(self):
        with open(os.path.join(local_dir_path, 'a0'), 'w') as outfile:
            json.dump('data', outfile)
        self.assertTrue(gpl5.exist_in_local(data_name='a0'))
        gpl5.delete_in_local(data_name='a0')
        self.assertFalse(gpl5.exist_in_local(data_name='a0'))
