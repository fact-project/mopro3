import subprocess as sp
import os
import logging
import pandas as pd

from io import StringIO


log = logging.getLogger(__name__)


def get_current_jobs(user=None):
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


def build_sbatch_command(
    executable,
    *args,
    stdout=None,
    stderr=None,
    job_name=None,
    partition=None,
    mail_address=None,
    mail_settings='FAIL',
    resources=None,
    walltime=None,
):
    command = []
    command.append('sbatch')

    if job_name:
        command.extend(['-J', job_name])

    if partition:
        command.extend(['-p', partition])

    if mail_address:
        command.append('--mail-user={}'.format(mail_address))

    command.append('--mail-type={}'.format(mail_settings))

    if stdout:
        command.extend(['-o', stdout])

    if stderr:
        command.extend(['-e', stderr])

    if resources:
        command.append('-l')
        command.append(','.join(
            '{}={}'.format(k, v)
            for k, v in resources.items()
        ))

    if walltime is not None:
        command.append('--time={}'.format(walltime))

    command.append(executable)
    command.extend(args)

    return command
