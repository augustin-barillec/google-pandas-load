#!/bin/bash

source clean.sh
clean_docs
cd docs
sphinx-build source build
google-chrome build/index.html
