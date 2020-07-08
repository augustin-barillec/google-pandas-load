#!/bin/bash

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