#!/bin/bash

PROJECT_DIR=$(pwd)
export PYTHONPATH=$PROJECT_DIR

cd tests/run
python all_with_coverage.py