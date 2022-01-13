import unittest
from tests import test_extra_informations

suite = unittest.TestSuite()
suite.addTest(test_extra_informations.ExtraInformationsTest('test_xload'))
unittest.TextTestRunner(verbosity=2).run(suite)
