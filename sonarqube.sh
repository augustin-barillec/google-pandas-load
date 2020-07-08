#!/bin/bash

echo "Start clean_sonarqube..."
bash -c "source clean.sh && clean_sonarqube"
echo "Ended clean_sonarqube"

echo "Start sonarqube container..."
docker run \
       -d  \
       --name sonarqube  \
       -p 9000:9000 \
       sonarqube:community
echo "Started sonarqube container"
