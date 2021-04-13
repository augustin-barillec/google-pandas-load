#!/bin/bash

PROJECT_DIR=$(pwd)

echo "Start clean_scannerwork..."
source clean.sh && clean_scannerwork
echo "Ended clean_scannerwork"

echo "Start sonar-scanner.."
mkdir .scannerwork
docker run \
    --rm \
    --network="host" \
    -v "$PROJECT_DIR:/usr/src" \
    sonarsource/sonar-scanner-cli
echo "Ended sonar-scanner"
