from mopro.database import (
    database,
    initialize_database,
    CorsikaSettings,
    CorsikaRun,
    Status,
)

initialize_database()


runs_per_bin = 10
delta_zd = 5
delta_az = 10

corsika_settings = (
    CorsikaSettings
    .select(CorsikaSettings.id)
    .where(CorsikaSettings.name == 'epos_fluka_iact_lapalma_winter')
    .where(CorsikaSettings.version == 76900)
)

options = dict(
    primary_particle=14,
    n_showers=1000,
    walltime=120,
    energy_min=100,
    energy_max=200e3,
    spectral_index=-2.7,
    max_radius=500,
    viewcone=0,
    reuse=20,
    corsika_settings=corsika_settings,
    status=Status.select(Status.id).where(Status.name == 'created'),
)


def generator():
    for min_az in range(0, 360, delta_az):
        yield dict(
            zenith_min=min_zd,
            zenith_max=min_zd + delta_zd,
            azimuth_min=min_az,
            azimuth_max=min_az + delta_az,
            **options,
        )


for i in range(runs_per_bin):
    for min_zd in range(0, 30, delta_zd):
        with database.connection_context():
            print(CorsikaRun.insert_many(
                generator(),
            ).execute())
