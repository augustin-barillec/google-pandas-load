#!/bin/bash

echo "Start sonarqube..."
./sonarqube.sh
echo "Ended sonarqube"

echo "Start sleep 30..."
sleep 30
echo "Ended sleep 30"

echo "Start sonar-scanner..."
./sonar-scanner.sh
echo "Ended sonar-scanner"
