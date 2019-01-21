from peewee import Proxy, Model, SqliteDatabase, MySQLDatabase
from peewee import (
    TextField, IntegerField, FloatField, Check,
    ForeignKeyField
)
from jinja2 import Template
from .config import config


database = Proxy()


class BaseModel(Model):
    class Meta:
        database = database


class Status(BaseModel):
    name = TextField(unique=True)


class CorsikaSettings(BaseModel):
    '''
    Attributes
    ----------
    version: int
        CORSIKA version in integer form e.g. 76900
    config_h: str
        compilation configuration for CORSIKA.
        Run ./coconut to create this file in the corsika include directory
    inputcard_template: str
        Jinja2 template for the inputcard
    '''
    version = IntegerField(default=76900)
    config_h = TextField()
    inputcard_template = TextField()

    def format_input_card(self, run, output_file):
        return Template(self.inputcard_template).render(
            run=run, output_file=output_file
        )


class CorsikaRun(BaseModel):
    '''
    Attributes
    ----------
    primary_particle: int
        primary particle id, e.g. 14 for proton, 1 for gamma
    zenith_min: float
        minimum zenith angle in degree
    zenith_max: float
        maximum zenith angle in degree, set equal to `zenith_min` for a fixed zenith
    azimuth_min: float
        minimum azimuth angle in degree
    azimuth_max: float
        maximum azimuth angle in degree, set equal to `zenith_min` for a fixed zenith
    energy_min: float
        minimum energy to simulate in GeV
    energy_max: float
        maximum energy to simulate in GeV
    spectral_index: float
        Spectral index, must be <= 0
    viewcone: float
        outer radius of the viewcone in degree
    reuse: int
        number of reuses for each shower
    '''
    corsika_settings = ForeignKeyField(CorsikaSettings)

    primary_particle = IntegerField()
    n_showers = IntegerField(default=5000)

    zenith_min = FloatField()
    zenith_max = FloatField()

    azimuth_min = FloatField()
    azimuth_max = FloatField()

    energy_min = FloatField()
    energy_max = FloatField()
    spectral_index = FloatField()

    viewcone = FloatField(default=0)

    reuse = IntegerField(default=1)
    max_radius = FloatField()
    bunch_size = IntegerField(default=1)

    class Meta:
        constraints = [
            Check('n_showers >= 1'),
            Check('zenith_min >= 0'),
            Check('zenith_max >= zenith_min'),
            Check('azimuth_min >= 0'),
            Check('azimuth_max >= azimuth_min'),
            Check('energy_min >= 0'),
            Check('energy_max >= energy_min'),
            Check('spectral_index <= 0'),
            Check('viewcone >= 0'),
            Check('reuse >= 1'),
            Check('reuse <= 20'),
            Check('max_radius >= 0'),
            Check('bunch_size >= 1'),
        ]

    def __repr__(self):
        return 'CorsikaRun(\n  ' + '\n  '.join([
            f'run_id={self.id}',
            f'corsika_settings={self.corsika_settings_id}',
            f'primary_particle={self.primary_particle}',
            f'n_showers={self.n_showers}',
            f'zenith_min={self.zenith_min} °',
            f'zenith_max={self.zenith_max} °',
            f'azimuth_min={self.azimuth_min} °',
            f'azimuth_max={self.azimuth_max} °',
            f'spectral_index={self.spectral_index}',
            f'viewcone={self.viewcone} °',
            f'reuse={self.reuse}',
            f'max_radius={self.max_radius} m',
            f'bunch_size={self.bunch_size}',
        ]) + '\n)'

    def __str__(self):
        return repr(self)


class CeresSettings(BaseModel):
    revision = IntegerField()
    rc_template = TextField()


class CeresRun(BaseModel):
    ceres_settings = ForeignKeyField(CeresSettings)
    corsika_run = ForeignKeyField(CorsikaRun)


status_names = (
    'created',
    'queued',
    'running',
    'success',
    'failed',
    'walltime_exceeded',
)


def initialize_database():
    db_config = config.database

    if db_config.kind == 'sqlite':
        database.initialize(SqliteDatabase(db_config.database))

    elif config.database.kind == 'mysql':
        database.initialize(MySQLDatabase(
            host=db_config.host or '127.0.0.1',
            port=db_config.port or 3306,
            user=db_config.user,
            password=db_config.password,
            database=db_config.database,
        ))

    else:
        raise ValueError(f'Unsupported database kind: "{db_config.kind}"')


def setup_database():
    with database.atomic():
        database.create_tables([
            Status, CorsikaSettings, CorsikaRun, CeresSettings, CeresRun
        ], safe=True)

    with database.atomic():
        for name in status_names:
            Status.get_or_create(name=name)
