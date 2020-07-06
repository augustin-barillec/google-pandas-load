#!/bin/bash

cd docs

rm -rf build && make html && google-chrome build/html/index.html