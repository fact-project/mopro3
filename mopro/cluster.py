from abc import ABCMeta, abstractmethod
import re
import logging

from .queries import update_job_status
from .database import CorsikaRun, CeresRun


class Cluster(metaclass=ABCMeta):
    log = logging.getLogger(__name__)

    @abstractmethod
    def submit_job(
        self,
        executable,
        *args,
        env=None,
        stdout=None,
        stderr=None,
        job_name=None,
        walltime=None,
    ):
        '''
        Add a new job to the cluster's queue
        '''

    @property
    @abstractmethod
    def n_running(self):
        '''
        Get the number of currently running jobs in the cluster
        '''

    @property
    @abstractmethod
    def n_queued(self):
        '''
        Get the number of currently queued jobs in the cluster
        '''

    @abstractmethod
    def get_running_jobs(self):
        '''
        Get the names of the currently running jobs in the cluster
        '''

    @abstractmethod
    def get_queued_jobs(self):
        '''
        Get the names of the currently queued jobs in the cluster
        '''

    @abstractmethod
    def kill_job(self, job_name):
        pass

    @abstractmethod
    def cancel_job(self, job_name):
        pass

    def set_to_created(self, job_name):
        m = re.match(r'mopro_(corsika|ceres)_(\d+)', job_name)
        if m is None:
            return

        program, job_id = m.groups()
        job_id = int(job_id)

        self.log.info(f'Setting job {job_id} to "created"')
        if program == 'corsika':
            update_job_status(CorsikaRun, job_id, 'created')
        elif program == 'ceres':
            update_job_status(CeresRun, job_id, 'created')

    def terminate(self):
        '''
        Cleanup all running and queued jobs, must
        reset the job state to `created`
        '''

        for name in self.get_running_jobs():
            self.kill_job(name)
            self.set_to_created(name)

        for name in self.get_queued_jobs():
            self.cancel_job(name)
            self.set_to_created(name)
