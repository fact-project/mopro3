import subprocess as sp
import os
import logging
import pandas as pd

from io import StringIO


log = logging.getLogger(__name__)


class SlurmCluster:
    def __init__(self, partitions, mail_address=None, mail_settings=None, memory=None):
        self.mail_address = mail_address
        self.mail_settings = mail_settings
        self.memory = memory
        self.partitions = [(v, k) for k, v in partitions.items()]
        self.partitions.sort(reverse=True)

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

        if self.memory:
            command.append(f'--mem={self.memory}')

        if walltime is not None:
            command.append(f'--time={walltime}')

        command.append(executable)
        command.extend(args)

        p = sp.run(command, stdout=sp.PIPE, stderr=sp.STDOUT, check=True, env=env)
        log.debug(f'Submitted new slurm jobs: {p.stdout.decode()}')

    def get_running_jobs(self):
        return self.get_current_jobs()['state'].value_counts().get('running', 0)

    def get_queued_jobs(self):
        return self.get_current_jobs()['state'].value_counts().get('pending', 0)

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

    def terminate(self):
        pass
