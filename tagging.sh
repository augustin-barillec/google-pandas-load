#!/bin/bash

source get_value.sh
VERSION=$(get_value_from_python_file version)

function tag(){
  git tag -a v$VERSION -m "version $VERSION"
}

function push_tag(){
  git push origin v$VERSION
}

function delete_tag(){
  git tag -d v$VERSION
  git push origin --delete v$VERSION
}