# mopro3

New FACT Monte Carlo Production, build for LIDO3 and local processing.

## Requirements

`mopro` downloads and builds the required simulation software automatically,
addionally the following programs are needed:

* `gcc` and `gfortran` >= 7.3
* `curl`
* `zstd`


## Setup

It's advised to create a new virtual or conda environment for mopro.

Virtualenv:
```
$ python3 -m venv $HOME/.local/venvs/mopro
$ . $HOME/.local/venvs/mopro/bin/activate
(mopro) $ pip install .
```

conda env:
```
$ conda create -n mopro python=3
$ conda activate mopro
(mopro) $ pip install .
```


Copy the configuration template `mopro_template.yaml` to `$HOME/mopro.yaml`,
and fill in the necessary credentials and other options.

To initialize the database, creating all tables and creating instances 
for rows for the different job stati, run

```
(mopro) $ python -m mopro.database
```

Start the submitter (-v for verbose output):

```
(mopro) $ python -m mopro.processing [-v]
```

Now, we can add CORSIKA and CeresSettings and submit runs.
Mopro will download and install the needed software as soon as a
corresponding run is started.
