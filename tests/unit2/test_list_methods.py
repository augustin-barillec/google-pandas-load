class LoaderTest(unittest.TestCase):
    def setUp(self):
        if not os.path.isdir(local_dir_path):
            os.makedirs(local_dir_path)

    def tearDown(self):
        shutil.rmtree(local_dir_path)