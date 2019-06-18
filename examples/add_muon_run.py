from mopro.database import (
    database,
    initialize_database,
    CorsikaSettings,
    CorsikaRun,
    Status,
)

initialize_database()

with database.connection_context():
    print('New job with id', CorsikaRun.insert(
        corsika_settings=CorsikaSettings.select(CorsikaSettings.id).limit(1),
        primary_particle=6,
        n_showers=1000,
        energy_min=1e3,
        energy_max=1e3,
        spectral_index=-2.0,
        zenith_min=0,
        zenith_max=0,
        azimuth_min=0,
        azimuth_max=0,
        max_radius=1,
        viewcone=1,
        reuse=1,
        status=Status.select(Status.id).where(Status.name == 'created'),
    ).execute(), 'inserted')
