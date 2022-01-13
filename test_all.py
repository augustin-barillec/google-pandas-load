import unittest
import coverage

cov = coverage.coverage(include='google_pandas_load/*')
cov.start()
suite = unittest.TestLoader().discover(start_dir='tests', pattern='*')
unittest.TextTestRunner(verbosity=2).run(suite)
cov.stop()
cov.report()
cov.html_report(directory='coverage')
cov.xml_report(outfile='coverage.xml')
