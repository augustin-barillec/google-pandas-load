import os
import unittest
import coverage

file_dir_path = os.path.dirname(__file__)
tests_dir_path = os.path.join(file_dir_path, '..')
root_project_dir_path = os.path.join(tests_dir_path, '..')
package_path = os.path.join(root_project_dir_path, 'google_pandas_load')
unit_dir_path = os.path.join(tests_dir_path, 'unit')
cov_dir_path = os.path.join(tests_dir_path, 'coverage')

cov = coverage.coverage(include=os.path.join(package_path, '*'))
cov.start()
suite = unittest.TestLoader().discover(start_dir=unit_dir_path, pattern='*')
unittest.TextTestRunner(verbosity=2).run(suite)
cov.stop()
cov.report()
cov.html_report(directory=cov_dir_path)
cov.xml_report(outfile=os.path.join(cov_dir_path, 'coverage.xml'))
