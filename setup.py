from setuptools import setup, find_namespace_packages

with open('README.rst') as f:
    README = f.read()

with open('requirements.txt') as f:
    REQUIREMENTS = f.read()

setup(
    name='google-pandas-load',
    version='2.0.0',
    author='Augustin Barillec',
    author_email='augustin.barillec@ysance.com',
    description=(
        'Wrapper for transferring big data from A to B, where A and B are '
        'distinct and chosen between BigQuery, Storage, a local folder and '
        'a pandas DataFrame.'),
    long_description=README,
    install_requires=REQUIREMENTS,
    packages=find_namespace_packages(include=['google_pandas_load*']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"]
)
