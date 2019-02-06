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
        primary_particle=14,
        n_showers=100,
        energy_min=100,
        energy_max=200e3,
        spectral_index=-2.7,
        zenith_min=0,
        zenith_max=5,
        azimuth_min=0,
        azimuth_max=10,
        max_radius=500,
        viewcone=0,
        reuse=20,
        status=Status.select(Status.id).where(Status.name == 'created'),
    ).execute(), 'inserted')
