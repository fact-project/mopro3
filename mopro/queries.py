from .database import (
    database,
    Status,
    CorsikaRun,
    CeresRun,
    CeresSettings,
    CorsikaSettings,
)


@database.connection_context()
def count_jobs(model, status='created'):
    return (
        model.select()
        .where(model.status == Status.get(name=status))
        .count()
    )


@database.connection_context()
def get_pending_jobs(max_jobs):
    created_id = Status.get(name='created').id
    success_id = Status.get(name='success').id

    jobs = list(
        CorsikaRun
        .select(CorsikaRun, CorsikaSettings)
        .join(CorsikaSettings)
        .where(CorsikaRun.status_id == created_id)
        .order_by(CorsikaRun.priority)
        .limit(max_jobs)
    )

    jobs.extend(list(
        CeresRun
        .select(
            CeresRun, CeresSettings, CorsikaRun.result_file
        )
        .join(CorsikaRun)
        .where(CorsikaRun.status_id == success_id)
        .where(CeresRun.status_id == created_id)
        .order_by(CeresRun.priority)
        .limit(max_jobs)
    ))

    jobs.sort(key=lambda j: j.priority)
    return jobs[:max_jobs]
