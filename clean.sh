#!/bin/bash


function clean_jupyter(){
  rm -rf docs/source/.ipynb_checkpoints
}

function clean_docs(){
  rm -rf docs/build
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
  rm -f .coverage coverage.xml
}

function clean_packaging(){
  rm -rf build dist
}

function clean_venv(){
  rm -rf venv
}

function clean_daily(){
  clean_jupyter
  clean_docs
  clean_scannerwork
  clean_coverage
  clean_packaging
}

function clean_all(){
  clean_daily
  clean_venv
  clean_sonarqube
}