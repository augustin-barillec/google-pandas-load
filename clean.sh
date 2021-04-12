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

function stop_sonarqube(){
  docker ps -aqf name="sonarqube" | xargs docker stop
}

function clean_sonarqube(){
  stop_sonarqube
  docker ps -aqf name="sonarqube" | xargs docker rm
}

function clean_coverage(){
  rm -rf coverage
  rm -f .coverage
  rm -f coverage.xml
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
  clean_editable
  clean_venv
  clean_sonarqube
}