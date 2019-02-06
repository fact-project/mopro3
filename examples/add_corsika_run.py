from mopro.database import (
    database,
    initialize_database,
    CorsikaSettings,
    CorsikaRun,
    Status,
)

initialize_database()

with database.connection_context():
    s = CorsikaSettings.get()

    r = CorsikaRun()
    r.corsika_settings = s
    r.primary_particle = 14
    r.n_showers = 10

    r.energy_min = 100
    r.energy_max = 200e3
    r.spectral_index = -2.7

    r.zenith_min = 0
    r.zenith_max = 5
    r.azimuth_min = 0
    r.azimuth_max = 10

    r.max_radius = 500
    r.viewcone = 0
    r.reuse = 20
    r.status = Status.get(name='created')
    r.save()
