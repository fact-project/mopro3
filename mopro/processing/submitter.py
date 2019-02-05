from threading import Thread, Event
import logging
import peewee
import pandas as pd

from ..database import Status, CorsikaRun, CeresRun
from ..queries import get_pending_jobs, count_jobs
from ..slurm import get_current_jobs

from .submit_corsika import submit_corsika_run


log = logging.getLogger(__name__)


class JobSubmitter(Thread):

    def __init__(
        self,
        interval,
        max_queued_jobs,
        mopro_directory,
        host,
        port,
        partitions,
        mail_address=None,
        mail_settings='NONE',
        memory=None,
        debug=False,
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
        mail_address: str
            mail address to receive the grid engines emails
        mail_setting: str
            mail setting for the grid engine
        '''
        super().__init__()
        self.event = Event()
        self.interval = interval
        self.max_queued_jobs = max_queued_jobs
        self.mopro_directory = mopro_directory
        self.host = host
        self.port = port
        self.mail_settings = mail_settings
        self.mail_address = mail_address
        self.debug = debug
        self.memory = memory
        self.partitions = list(v, k for k, v in partitions.items())
        self.partitions.sort(reverse=True)

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
        if self.debug:
            current_jobs = pd.DataFrame({'state': []})
        else:
            current_jobs = get_current_jobs()

        running_jobs = current_jobs.query('state == "running"')
        queued_jobs = current_jobs.query('state == "pending"')
        log.debug('{} jobs running'.format(len(running_jobs)))
        log.debug('{} jobs queued'.format(len(queued_jobs)))
        log.debug('{} pending CORSIKA jobs in database'.format(
            count_jobs(CorsikaRun, status='created')
        ))
        log.debug('{} pending CERES jobs in database'.format(
            count_jobs(CeresRun, status='created')
        ))

        if len(queued_jobs) < self.max_queued_jobs:
            max_jobs = self.max_queued_jobs - len(queued_jobs)
            pending_jobs = get_pending_jobs(max_jobs=max_jobs)

            for job in pending_jobs:
                if self.event.is_set():
                    break

                kwargs = {
                    'mopro_directory': self.mopro_directory,
                    'submitter_host': self.host,
                    'submitter_port': self.port,
                    'mail_settings': self.mail_settings,
                    'mail_address': self.mail_address,
                    'memory': self.memory,
                }

                try:
                    kwargs['walltime'] = self.walltime_to_partition(job.walltime)
                    if isinstance(job, CorsikaRun):
                        submit_corsika_run(job, **kwargs)
                        log.info(f'Submitted new CORSIKA job with id {job.id}')
                except:
                    log.exception('Could not submit job')
                    job.status = Status.get(name='failed')
                    job.save()

    def walltime_to_partition(self, walltime):
        for max_walltime, partition in self.partitions:
            if walltime <= max_walltime:
                return partition
        raise ValueError('Walltime to long for available partitions')
