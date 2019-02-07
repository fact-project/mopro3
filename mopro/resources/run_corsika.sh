#!/bin/bash

echo "python executable"
which python

echo -e "\n\npython version"
python --version

echo -e "\n\nMOPRO env variables"
env | grep MOPRO

echo -e "\n\nAvailable space in /tmp"
df -h /tmp

echo -e "\n\nStart processing"
python -m mopro.processing.run_corsika
