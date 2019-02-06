from mopro.database import (
    database,
    initialize_database,
    CeresSettings,
    CorsikaRun,
    CeresRun,
    Status,
)

initialize_database()

with database.connection_context():
    for corsika_run in CorsikaRun.select(CorsikaRun.id):
        print('New job with id', CeresRun.insert(
            ceres_settings=CeresSettings.select(CeresSettings.id).limit(1),
            corsika_run=corsika_run,
            status=Status.select(Status.id).where(Status.name == 'created'),
        ).execute(), 'inserted')
