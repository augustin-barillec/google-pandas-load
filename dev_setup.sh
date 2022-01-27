#!/bin/bash

echo "Starting update..."
apt-get update
echo "Ended update"

echo "Installing pandoc..."
apt-get install pandoc
echo "Installed pandoc"
