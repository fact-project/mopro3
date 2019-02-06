from .database import (
    database,
    Status,
    CorsikaRun,
    CeresRun,
    CeresSettings,
    CorsikaSettings,
)


@database.connection_context()
def update_job_status(model, job_id, new_status='created'):
    # subquery for new status
    status = Status.select().where(Status.name == new_status)
    return (
        model.update(status=status)
        .where(model.id == job_id)
        .execute()
    )


@database.connection_context()
def count_jobs(model, status='created'):
    return (
        model.select()
        .where(model.status == Status.select().where(Status.name == status))
        .count()
    )


@database.connection_context()
def get_pending_jobs(max_jobs):
    # subqueries for process state
    created = Status.select().where(Status.name == 'created')
    success = Status.select().where(Status.name == 'success')

    # first get all pending corsika jobs
    jobs = list(
        CorsikaRun
        .select(
            CorsikaRun,
            CorsikaSettings.name,
            CorsikaSettings.version,
            CorsikaSettings.id,
            CorsikaSettings.inputcard_template,
        )
        .join(CorsikaSettings)
        .where(CorsikaRun.status == created)
        .order_by(CorsikaRun.priority)
        .limit(max_jobs)
    )

    # then get all ceres jobs, where the corsika run was already successfull
    jobs.extend(list(
        CeresRun
        .select(
            CeresRun,
            CeresSettings.id, CeresSettings.name, CeresSettings.revision,
            CeresSettings.off_target_distance, CeresSettings.diffuse,
            CorsikaRun.id, CorsikaRun.result_file,
            CorsikaRun.zenith_min, CorsikaRun.zenith_max,
            CorsikaRun.azimuth_min, CorsikaRun.azimuth_max,
            CorsikaRun.primary_particle,
            CorsikaSettings.name, CorsikaSettings.version,
        )
        .join(CeresSettings)
        .switch(CeresRun)
        .join(CorsikaRun)
        .join(CorsikaSettings)
        .where(CorsikaRun.status == success)
        .where(CeresRun.status == created)
        .order_by(CeresRun.priority)
        .limit(max_jobs)
    ))

    # sort jobs by priority
    jobs.sort(key=lambda j: j.priority)
    # return at most max_jobs jobs
    return jobs[:max_jobs]
