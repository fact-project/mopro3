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

    corsika_runs = list(
        CorsikaRun
        .select(CorsikaRun.id).alias('corsika_run_id')
        .join(CeresRun, JOIN.LEFT_OUTER)
        .join(CeresSettings, JOIN.LEFT_OUTER)
        .where((CeresRun.id == None) | (CeresSettings.id != ceres_settings))
        .distinct()
    )

    print('CORSIKA without corresponding ceres run:', len(corsika_runs))
    print(corsika_runs[0])

    print(CeresRun.insert_many((
        dict(
            corsika_run_id=corsika_run.id,
            ceres_settings=ceres_settings,
            status=Status.select(Status.id).where(Status.name == 'created'),
            off_target_distance=6,
            diffuse=True,
        ) for corsika_run in corsika_runs),
    ).execute())
