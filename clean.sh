#!/bin/bash

PROJECT_DIR=$(pwd)

function clean_docs(){
  cd docs
  rm -rf build
  cd $PROJECT_DIR
}

function clean_scannerwork(){
  rm -rf .scannerwork
}

function clean_sonarqube(){
  docker ps -aqf name="sonarqube" | xargs docker stop
  docker ps -aqf name="sonarqube" | xargs docker rm
}

function clean_coverage(){
  cd tests
  rm -rf coverage
  cd run
  rm -f .coverage
  cd $PROJECT_DIR
}

function clean_build(){
  rm -rf build dist
}

function clean_editable(){
  rm -rf google_pandas_load.egg-info
}


function clean_venv(){
  clean_editable
  rm -rf venv
}

function clean_daily(){
  clean_docs
  clean_scannerwork
  clean_coverage
  clean_build
}

function clean_all(){
  clean_daily
  clean_sonarqube
  clean_editable
  clean_venv
}