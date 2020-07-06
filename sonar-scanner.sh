#!/bin/bash

PROJECT_DIR=$(pwd)

docker run \
    --rm \
    --network="host" \
    -v "$PROJECT_DIR:/usr/src" \
    sonarsource/sonar-scanner-cli
