import subprocess as sp
import os
import logging
import pandas as pd
from io import StringIO

from .cluster import Cluster


class SlurmCluster(Cluster):
    log = logging.getLogger(__name__)

    def __init__(self, partitions, mail_address=None, mail_settings=None, memory=None):
        self.mail_address = mail_address
        self.mail_settings = mail_settings
        self.partitions = [(v, k) for k, v in partitions.items()]
        self.partitions.sort()

    def walltime_to_partition(self, walltime):
        for max_walltime, partition in self.partitions:
            if walltime <= max_walltime:
                return partition
        raise ValueError('Walltime to long for available partitions')

    def submit_job(
        self,
        executable,
        *args,
        env=None,
        stdout=None,
        stderr=None,
        job_name=None,
        walltime=None,
        memory=None,
    ):
        command = []
        command.append('sbatch')

        if job_name:
            command.extend(['-J', job_name])

        partition = self.walltime_to_partition(walltime)
        command.extend(['-p', partition])

        if self.mail_address:
            command.append(f'--mail-user={self.mail_address}')

        if self.mail_settings:
            command.append(f'--mail-type={self.mail_settings}')

        if stdout:
            command.extend(['-o', stdout])

        if stderr:
            command.extend(['-e', stderr])

        if memory:
            command.append(f'--mem={self.memory}')

        if walltime is not None:
            command.append(f'--time={walltime}')

        command.append(executable)
        command.extend(args)

        p = sp.run(command, stdout=sp.PIPE, stderr=sp.STDOUT, check=True, env=env)
        self.log.debug(f'Submitted new slurm jobs: {p.stdout.decode().strip()}')

    def kill_job(self, job_name):
        p = sp.run(['scancel', '-n', job_name], stdout=sp.PIPE, stderr=sp.STDOUT)
        stdout = p.stdout.decode().strip()
        self.log.debug(f'Canceled slurm job {stdout}')

    def cancel_job(self, job_name):
        p = sp.run(['scancel', '-n', job_name], stdout=sp.PIPE, stderr=sp.STDOUT)
        stdout = p.stdout.decode().strip()
        self.log.debug(f'Canceled slurm job {stdout}')

    @property
    def n_running(self):
        return int(self.get_current_jobs()['state'].value_counts().get('running', 0))

    @property
    def n_queued(self):
        return int(self.get_current_jobs()['state'].value_counts().get('pending', 0))

    def get_running_jobs(self):
        jobs = self.get_current_jobs()
        return list(jobs.loc[jobs['state'] == 'running', 'name'])

    def get_queued_jobs(self):
        jobs = self.get_current_jobs()
        return list(jobs.loc[jobs['state'] == 'pending', 'name'])

    def get_current_jobs(self, user=None):
        ''' Return a dataframe with current jobs of user '''
        user = user or os.environ['USER']
        fmt = '%i,%j,%P,%S,%T,%p,%u,%V'
        csv = StringIO(sp.check_output([
            'squeue', '-u', user, '-o', fmt
        ]).decode())

        df = pd.read_csv(csv)
        df.rename(inplace=True, columns={
            'STATE': 'state',
            'USER': 'owner',
            'NAME': 'name',
            'JOBID': 'job_number',
            'SUBMIT_TIME': 'submission_time',
            'PRIORITY': 'priority',
            'START_TIME': 'start_time',
            'PARTITION': 'queue',
        })
        df['state'] = df['state'].str.lower()
        df['start_time'] = pd.to_datetime(df['start_time'])
        df['submission_time'] = pd.to_datetime(df['submission_time'])

        return df
