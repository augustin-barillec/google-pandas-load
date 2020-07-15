#!/bin/bash

echo "Start sonarqube..."
./sonarqube.sh
echo "Ended sonarqube"

echo "Start sleep 60..."
sleep 60
echo "Ended sleep 60"

echo "Start sonar-scanner..."
./sonar-scanner.sh
echo "Ended sonar-scanner"
