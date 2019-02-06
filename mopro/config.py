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

SubmitterConfig = namedtuple(
    'SubmitterConfig',
    ['interval', 'max_queued_jobs', 'host', 'port', 'mode'],
)
SubmitterConfig.__new__.__defaults__ = (
    60, 300, 'localhost', 1337, 'local'
)

SlurmConfig = namedtuple(
    'SlurmConfig',
    ['partitions', 'cpus', 'memory', 'mail_settings', 'mail_address'],
)
SlurmConfig.__new__.__defaults__ = (
    1, '8G', 'NONE', os.environ['USER'] + '@localhost'
)

LocalConfig = namedtuple('LocalConfig', ['cores'])
LocalConfig.__new__.__defaults__ = (
    None,
)


class Config():
    corsika_password = os.environ.get('CORSIKA_PASSWORD', '')
    fluka_id = os.environ.get('FLUKA_ID', '')
    fluka_password = os.environ.get('FLUKA_PASSWORD', '')
    database = DatabaseConfig()
    submitter = SubmitterConfig()
    local = LocalConfig()
    slurm = SlurmConfig(partitions=[])
    mopro_directory = os.path.abspath(os.getcwd())
    debug = False

    def __init__(self, paths=default_paths):
        for path in paths:
            if os.path.isfile(path):
                self.load_yaml(path)

    def load_yaml(self, path):
        with open(path, 'rb') as f:
            config = yaml.load(f)

        self.parse_dict(config)

    def parse_dict(self, config):
        self.debug = config.get('debug', False)

        corsika = config.get('corsika', {})
        self.corsika_password = corsika.get('password', '') or self.corsika_password

        fluka = config.get('fluka', {})
        self.fluka_id = fluka.get('id', '') or self.fluka_id
        self.fluka_password = fluka.get('password', '') or self.fluka_password

        if config.get('database') is not None:
            self.database = DatabaseConfig(**config['database'])

        if config.get('submitter') is not None:
            self.submitter = SubmitterConfig(**config['submitter'])

        if config.get('cluster') is not None:
            self.slurm = SlurmConfig(**config['slurm'])

        if config.get('partitions') is not None:
            self.partitions = config['partitions']

        if config.get('local') is not None:
            self.local = LocalConfig(**config['local'])

        self.mopro_directory = config.get('mopro_directory') or self.mopro_directory
        self.mopro_directory = os.path.abspath(self.mopro_directory)


config = Config()
