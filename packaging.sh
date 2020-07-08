#!/bin/bash

function build {
source clean.sh
clean_build
python setup.py bdist_wheel
}

function upload_to_testpypi {
twine upload --repository testpypi dist/*
}

