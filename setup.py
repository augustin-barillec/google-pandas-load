from setuptools import setup, find_namespace_packages

with open('README.rst') as f:
    README = f.read()

with open('requirements.txt') as f:
    REQUIREMENTS = f.read()

setup(
    name='google-pandas-load',
    version='0.1.0',
    author='Augustin Barillec',
    author_email='augustin.barillec@ysance.com',
    description="""
    Wrapper for conveying big data from A to B, where A and B are distinct 
    and chosen among BigQuery, Storage, a local folder or pandas.
    """,
    long_description=README,
    install_requires=REQUIREMENTS,
    packages=find_namespace_packages(include=['google_pandas_load*']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"]
)
