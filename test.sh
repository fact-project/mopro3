#!/bin/bash

set -e


rm -f database.sqlite
rm -rf ~/mopro_test/{logs/ceres,logs/corsika,corsika,ceres}

python -m mopro.database
python -m mopro.scripts.add_corsika_version \
	muons \
	76900 \
	examples/epos_urqmd_iact.h \
	examples/muon_template.txt \
	-a examples/epos_files.tar.gz

python examples/add_ceres_settings.py

for i in $(seq 1 1 10); do
	python examples/add_corsika_run.py
done

python examples/add_ceres_runs.py


python -m mopro.processing -v
