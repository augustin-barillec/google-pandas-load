from setuptools import setup, find_namespace_packages

with open('README.md') as f:
    README = f.read()

with open('requirements.txt') as f:
    REQUIREMENTS = f.read()

setup(
    name='google-pandas-load',
    version='1.0.dev1',
    author='Augustin Barillec',
    author_email='augustin.barillec@ysance.com',
    description="""
    Wrapper for conveying big data between A and B, with A and B distinct 
    amongst BigQuery, Storage, a local folder and pandas.
    """,
    long_description=README,
    install_requires=REQUIREMENTS,
    packages=find_namespace_packages(include=['google_pandas_load*']),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"]
)
