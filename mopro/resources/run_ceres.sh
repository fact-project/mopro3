#!/bin/bash

which python
python --version
env | grep MOPRO
echo "PATH: $PATH"
echo "LD_LIBRARY_PATH: $LD_LIBRARY_PATH"
python -m mopro.processing.run_ceres
