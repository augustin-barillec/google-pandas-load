#!/bin/bash

function clean_docs(){
  rm -rf docs/build
}

function clean_coverage(){
  rm -rf coverage
  rm .coverage coverage.xml
}

function clean_packaging(){
  rm -rf build dist google_pandas_load.egg-info
}

function clean(){
  clean_docs
  clean_coverage
  clean_packaging
}
