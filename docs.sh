#!/bin/bash

source clean.sh
clean_docs
cd docs
make html
google-chrome build/html/index.html
