#!/bin/bash

PROJECT_DIR=$(pwd)

echo "Start clean_scannerwork..."
bash -c "source clean.sh && clean_scannerwork"
echo "Ended clean_scannerwork"

docker run \
    --rm \
    --network="host" \
    -v "$PROJECT_DIR:/usr/src" \
    sonarsource/sonar-scanner-cli
