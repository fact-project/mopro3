from threading import Thread, Event
import logging
import peewee

from ..database import Status, CorsikaRun, CeresRun, database
from ..queries import get_pending_jobs, count_jobs, update_job_status
from .corsika import prepare_corsika_job


log = logging.getLogger(__name__)


class JobSubmitter(Thread):

    def __init__(
        self,
        interval,
        max_queued_jobs,
        mopro_directory,
        host,
        port,
        cluster,
    ):
        '''
        Parametrs
        ----------
        interval: int
            number of seconds to wait between submissions
        max_queued_jobs: int
            Maximum number of jobs in the queue of the grid engine
            No new jobs are submitted if the number of jobs in the queue is
            higher than this value
        mopro_directory: str
            patch to the basic structure for erna. Logfiles, jars, xmls and
            analysis output are stored in subdirectories to this directory.
        host: str
            hostname of the submitter node
        port: int
            port for the zmq communication
        '''
        super().__init__()
        self.event = Event()
        self.interval = interval
        self.max_queued_jobs = max_queued_jobs
        self.mopro_directory = mopro_directory
        self.host = host
        self.port = port
        self.cluster = cluster

    def run(self):
        while not self.event.is_set():
            try:
                self.process_pending_jobs()
            except peewee.OperationalError:
                log.exception('Lost database connection')
            except Exception as e:
                log.exception('Error during submission: {}'.format(e))
            self.event.wait(self.interval)

    def terminate(self):
        self.event.set()

    def process_pending_jobs(self):
        '''
        Fetches pending runs from the processing database
        and submits them using qsub if not to many jobs are running already.
        '''
        pending_corsika = count_jobs(CorsikaRun, status='created')
        pending_ceres = count_jobs(CeresRun, status='created')

        n_queued = self.cluster.n_queued
        log.debug(f'{self.cluster.n_running} jobs running')
        log.debug(f'{n_queued} jobs queued')
        log.debug(f'{pending_corsika} pending CORSIKA jobs in database')
        log.debug(f'{pending_ceres} pending CERES jobs in database')

        new_jobs = self.max_queued_jobs - n_queued
        if new_jobs > 0:
            pending_jobs = get_pending_jobs(max_jobs=new_jobs)

            for job in pending_jobs:
                if self.event.is_set():
                    break

                kwargs = {
                    'mopro_directory': self.mopro_directory,
                    'submitter_host': self.host,
                    'submitter_port': self.port,
                }

                try:
                    if isinstance(job, CorsikaRun):
                        self.cluster.submit_job(**prepare_corsika_job(job, **kwargs))
                        log.info(f'Submitted new CORSIKA job with id {job.id}')

                    update_job_status(type(job), job.id, 'queued')
                except:
                    log.exception('Could not submit job')
                    update_job_status(type(job), job.id, 'failed')

