#!/bin/bash

docker ps -aqf name="sonarqube" | xargs docker stop
docker ps -aqf name="sonarqube" | xargs docker rm