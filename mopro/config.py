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
from collections import namedtuple

yaml = YAML(typ='safe')

# default paths
default_paths = [
    os.path.join(os.environ['HOME'], 'mopro.yaml'),
    os.path.join(os.getcwd(), 'mopro.yaml'),
]


DatabaseConfig = namedtuple(
    'DatabaseConfig',
    ['kind', 'host', 'port', 'user', 'password', 'database']
)
# provide defaults for namedtuple fields
# default is an in-memory sqlite database
DatabaseConfig.__new__.__defaults__ = ('sqlite', None, None, None, None, ':memory:')


class Config():
    corsika_password = os.environ.get('CORSIKA_PASSWORD', '')
    fluka_id = os.environ.get('FLUKA_ID', '')
    fluka_password = os.environ.get('FLUKA_PASSWORD', '')
    database = DatabaseConfig()

    def __init__(self, paths=default_paths):
        for path in paths:
            if os.path.isfile(path):
                self.load_yaml(path)

    def load_yaml(self, path):
        with open(path, 'rb') as f:
            config = yaml.load(f)

        self.parse_dict(config)

    def parse_dict(self, config):
        corsika = config.get('corsika', {})
        self.corsika_password = corsika.get('password', '') or self.corsika_password

        fluka = config.get('fluka', {})
        self.fluka_id = fluka.get('id', '') or self.fluka_id
        self.fluka_password = fluka.get('password', '') or self.fluka_password

        if config.get('database') is not None:
            self.database = DatabaseConfig(**config['database'])


config = Config()
