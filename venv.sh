#!/bin/bash

set -e

echo "Deleting venv..."
source clean.sh
clean_venv
echo "Deleted venv"

echo "Creating venv..."
python3 -m venv venv
echo "Created venv"

echo "Activating venv..."
source venv/bin/activate
echo "Activated venv"

echo "Installing wheel..."
pip install wheel
echo "Installed wheel"

echo "Installing requirements-dev..."
pip install -r requirements-dev.txt
echo "Installed requirements-dev"

echo "Installing editable..."
pip install -e .
echo "Installed editable"