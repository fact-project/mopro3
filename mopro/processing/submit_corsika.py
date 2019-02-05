import os
import logging
import subprocess as sp
from pkg_resources import resource_filename
from multiprocessing import Process

from ..corsika_utils import primary_id_to_name
from ..slurm import build_sbatch_command
from ..config import config
from ..database import Status, CorsikaSettings
from ..installation import install_corsika

log = logging.getLogger(__name__)


def build_directory_name(corsika_run):
    return os.path.join(
        str(corsika_run.corsika_settings.version),
        corsika_run.corsika_settings.name,
        primary_id_to_name(corsika_run.primary_particle),
        f'{corsika_run.id // 1000:08d}'
    )


def build_basename(corsika_run):
    return 'corsika_{primary}_run_{run:08d}_az{min_az:03.0f}-{max_az:03.0f}_zd{min_zd:02.0f}-{max_zd:02.0f}'.format(
        primary=primary_id_to_name(corsika_run.primary_particle),
        run=corsika_run.id,
        min_az=corsika_run.azimuth_min,
        max_az=corsika_run.azimuth_max,
        min_zd=corsika_run.zenith_min,
        max_zd=corsika_run.zenith_max,
    )


def run_local(script, env, stdout):
    with open(stdout, 'wb') as f:
        sp.Popen([script], env=env, stdout=f, stderr=f)


def submit_corsika_run(
    corsika_run,
    mopro_directory,
    submitter_host,
    submitter_port,
    mail_settings,
    mail_address,
    partition,
    memory,
):

    directory = build_directory_name(corsika_run)
    output_dir = os.path.join(mopro_directory, 'corsika', directory)
    os.makedirs(output_dir, exist_ok=True)

    log_dir = os.path.join(mopro_directory, 'logs', 'corsika', directory)
    os.makedirs(log_dir, exist_ok=True)

    basename = build_basename(corsika_run)
    output_file = basename + '.eventio'

    corsika_dir = os.path.join(
        mopro_directory, 'software', 'corsika',
        str(corsika_run.corsika_settings.version),
        str(corsika_run.corsika_settings.name),
    )
    if not os.path.exists(corsika_dir):
        corsika_settings =  CorsikaSettings.get(id=corsika_run.corsika_settings_id)
        install_corsika(
            corsika_dir, corsika_settings.config_h, corsika_settings.version,
            corsika_settings.additional_files,
        )

    script = resource_filename('mopro', 'resources/run_corsika.sh')
    cmd = build_sbatch_command(
        script,
        job_name='mopro_corsika_{}'.format(corsika_run.id),
        stdout=os.path.join(log_dir, basename + '.log'),
        walltime=corsika_run.walltime,
        mail_settings=mail_settings,
        mail_address=mail_address,
        memory=memory,
        partition=partition,
    )
    inputcard = os.path.join(output_dir, basename + '.input')
    with open(inputcard, 'w') as f:
        content = corsika_run.corsika_settings.format_input_card(corsika_run, output_file)
        f.write(content)

    env = os.environ.copy()
    env.update({
        'MOPRO_JOB_ID': str(corsika_run.id),
        'MOPRO_CORSIKA_DIR': corsika_dir,
        'MOPRO_INPUTCARD': inputcard,
        'MOPRO_OUTPUTDIR': output_dir,
        'MOPRO_OUTPUTFILE': output_file,
        'MOPRO_WALLTIME': str(corsika_run.walltime * 60),
        'MOPRO_SUBMITTER_HOST': submitter_host,
        'MOPRO_SUBMITTER_PORT': str(submitter_port),
    })

    if not config.debug:
        output = sp.check_output(
            cmd,
            env=env,
        )
        log.debug(output.decode().strip())
    else:
        log.info(cmd)
        Process(
            target=run_local,
            args=(script, env, os.path.join(log_dir, basename + '.log'))
        ).run()

    corsika_run.status = Status.get(name='queued')
    corsika_run.save()
