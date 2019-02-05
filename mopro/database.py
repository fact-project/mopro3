from peewee import Proxy, Model, SqliteDatabase, MySQLDatabase, ConnectionContext
from peewee import (
    TextField, IntegerField, FloatField, Check,
    ForeignKeyField, BooleanField,
    BlobField,
)
from jinja2 import Template
import os

from .config import config


class ProxyWithContext(Proxy):
    # fix decorator usage with Proxy
    def connection_context(self):
        return ConnectionContext(self)


database = ProxyWithContext()


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
    name = TextField()
    version = IntegerField(default=76900)
    config_h = TextField()
    inputcard_template = TextField()
    additional_files = BlobField(null=True)

    def format_input_card(self, run, output_file):
        return Template(self.inputcard_template).render(
            run=run, output_file=output_file
        )

    class Meta:
        database = database
        indexes = (
            (('name', 'version'), True),  # unique index on name/version
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
    # CORSIKA related fields
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

    # processing related fields
    priority = IntegerField(default=5)
    duration = IntegerField(null=True)
    status = ForeignKeyField(Status)
    walltime = IntegerField(default=360)
    result_file = TextField(null=True)

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
    name = TextField(unique=True)
    revision = IntegerField()
    rc_template = TextField()

    # files
    reflector_file = TextField()
    mirror_reflectivity_file = TextField()
    pde_file = TextField()
    cone_angular_acceptance_file = TextField()
    cone_transmission_file = TextField()
    nsb_file = TextField(null=True)
    pixel_delay_file = TextField()
    route_ac_file = TextField()

    # settings
    psf_sigma = FloatField()
    apd_dead_time = FloatField()
    apd_recovery_time = FloatField()
    apd_cross_talk = FloatField()
    apd_afterpulse_probability_1 = FloatField()
    apd_afterpulse_probability_2 = FloatField()
    excess_noise = FloatField()
    nsb_rate = FloatField(null=True)
    additional_photon_acceptance = FloatField()
    dark_count_rate = FloatField()
    pulse_shape_function = TextField()
    residual_time_spread = FloatField()
    gapd_time_jitter = FloatField()

    def format_rc(self, run, resource_directory):
        return Template(self.rc_template).render(
            settings=self, run=run, resource_directory=resource_directory
        )

    def write_files(self, resource_directory):
        files = {
            'reflector.txt': self.reflector_file,
            'mirror-reflectivity.txt': self.mirror_reflectivity_file,
            'pde.txt': self.pde_file,
            'cone-angular-acceptance.txt': self.cone_angular_acceptance_file,
            'cone-transmission.txt': self.cone_transmission_file,
            'nsb.txt': self.nsb_file,
            'pixel-delays.csv': self.pixel_delay_file,
            'route-ac.txt': self.route_ac_file,
        }
        os.makedirs(resource_directory, exists_ok=True)
        for name, content in files.items():
            with open(os.path.join(resource_directory, name), 'w') as f:
                f.write(content)


class CeresRun(BaseModel):
    ceres_settings = ForeignKeyField(CeresSettings)
    # input file
    corsika_run = ForeignKeyField(CorsikaRun)

    # per run settings
    off_target_distance = FloatField(default=6)
    diffuse = BooleanField(default=True)

    # processing related fields
    duration = IntegerField(null=True)
    status = ForeignKeyField(Status)
    walltime = IntegerField(default=360)
    priority = IntegerField(default=5)
    result_file = TextField(null=True)


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


if __name__ == '__main__':
    initialize_database()
    setup_database()
