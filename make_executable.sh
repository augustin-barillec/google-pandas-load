#!/bin/bash

for entry in *.sh
do
    echo $entry
    chmod +x $entry
done