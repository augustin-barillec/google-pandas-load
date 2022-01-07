import unittest


class BaseClassTest(unittest.TestCase):
    def setUp(self):
        clean()
        create_local_subfolder()

    def tearDown(self):
        clean()
