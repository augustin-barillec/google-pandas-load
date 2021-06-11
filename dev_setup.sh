#!/bin/bash

echo "Starting update..."
apt-get update
echo "Ended update"

echo "Installing python3-venv..."
apt-get install python3-venv
echo "Installed python3-venv"

echo "Installing pandoc..."
apt-get install pandoc
echo "Installed pandoc"
