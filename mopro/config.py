'''
Config class that loads yaml config files

At import, it tries to load $HOME/.mopro.yaml and $(pwd)/mopro.yaml

It also checks for the following environment variables:

* CORSIKA_PASSWORD
* CORSIKA_VERSION
* FLUKA_ID
* FLUKA_PASSWORD
'''
import os
from ruamel.yaml import YAML

yaml = YAML(typ='safe')


class Config:
    corsika_password = os.environ.get('CORSIKA_PASSWORD', '')
    corsika_version = int(os.environ.get('CORSIKA_VERSION', 76900))
    fluka_id = os.environ.get('FLUKA_ID', '')
    fluka_password = os.environ.get('FLUKA_PASSWORD', '')

    @classmethod
    def load_yaml(cls, path):
        with open(path, 'rb') as f:
            config = yaml.load(f)

        cls.parse_dict(config)

    @classmethod
    def parse_dict(cls, config):
        corsika = config.get('corsika', {})
        cls.corsika_password = corsika.get('password', '') or cls.corsika_password
        cls.corsika_version = corsika.get('version', '') or cls.corsika_version

        fluka = config.get('fluka', {})
        cls.fluka_id = fluka.get('id', '') or cls.fluka_id
        cls.fluka_password = fluka.get('password', '') or cls.fluka_password


config_paths = [
    os.path.join(os.environ['HOME'], 'mopro.yaml'),
    os.path.join(os.getcwd(), 'mopro.yaml'),
]

for path in config_paths:
    if os.path.isfile(path):
        Config.load_yaml(path)
