from setuptools import setup, find_namespace_packages
from version import version

with open('README.rst') as f:
    README = f.read()

with open('requirements.txt') as f:
    REQUIREMENTS = f.read()

setup(
    name='google-pandas-load',
    version=version,
    author='Augustin Barillec',
    author_email='augustin.barillec@gmail.com',
    description=(
        'Wrapper for transferring data from A to B, where A and B are '
        'distinct and chosen between BigQuery, Storage, a local directory '
        'and pandas.'),
    long_description=README,
    install_requires=REQUIREMENTS,
    packages=find_namespace_packages(include=['google_pandas_load*']),
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX :: Linux'],
    project_urls={
        'Documentation':
            'https://google-pandas-load.readthedocs.io/en/latest/',
        'Source': 'https://github.com/augustin-barillec/google-pandas-load'}
)
