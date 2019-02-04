#!/bin/bash

which python
python --version
env | grep MOPRO
python -m mopro.processing.run_corsika
