from mopro.database import (
    database,
    initialize_database,
    CeresSettings,
    CorsikaRun,
    CeresRun,
    Status,
)
from peewee import JOIN

initialize_database()

with database.connection_context():
    ceres_settings = CeresSettings.select(CeresSettings.id).limit(1)
    corsika_runs = (
        CorsikaRun
        .select(CorsikaRun.id)
        .join(CeresRun, JOIN.LEFT_OUTER)
        .join(CeresSettings, JOIN.LEFT_OUTER)
        .where((CeresRun.id == None) | (CeresSettings.id != ceres_settings))
        .distinct()
    )
    print(corsika_runs.count())
    for corsika_run in corsika_runs:
        print(corsika_run.id)
        print('New job with id', CeresRun.insert(
            ceres_settings=ceres_settings,
            corsika_run=corsika_run,
            status=Status.select(Status.id).where(Status.name == 'created'),
            off_target_distance=6,
            diffuse=True,
        ).execute(), 'inserted')
