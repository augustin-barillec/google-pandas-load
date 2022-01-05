#!/bin/bash


function clean_jupyter(){
  rm -rf .ipynb_checkpoints
  rm -rf docs/source/.ipynb_checkpoints
}

function clean_docs(){
  rm -rf docs/build
}

function clean_coverage(){
  rm -rf coverage
  rm -f .coverage coverage.xml
}

function clean_packaging(){
  rm -rf build dist google_pandas_load.egg-info
}

function clean_venv(){
  rm -rf venv
}

function clean_daily(){
  clean_jupyter
  clean_docs
  clean_coverage
  clean_packaging
}

function clean_all(){
  clean_daily
  clean_venv
}
