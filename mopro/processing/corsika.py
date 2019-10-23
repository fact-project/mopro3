import os
import logging
from pkg_resources import resource_filename
import shutil

from ..database import database
from ..database import CorsikaSettings
from ..installation import install_corsika

log = logging.getLogger(__name__)


def prepare_corsika_job(
    corsika_run,
    mopro_directory,
    submitter_host,
    submitter_port,
    tmp_dir=None,
):

    script = resource_filename('mopro', 'resources/run_corsika.sh')
    directory = corsika_run.directory_name
    basename = corsika_run.basename

    output_dir = os.path.join(mopro_directory, directory)
    log_dir = os.path.join(mopro_directory, 'logs', directory)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, basename + '.log')
    output_file = basename + '.eventio'
    inputcard_file = os.path.join(output_dir, basename + '.input')

    corsika_dir = os.path.join(
        mopro_directory, 'software', 'corsika',
        str(corsika_run.corsika_settings.version),
        str(corsika_run.corsika_settings.name),
    )
    if not os.path.exists(corsika_dir):
        install_log_dir = os.path.join(mopro_directory, 'logs', 'installation')
        os.makedirs(install_log_dir, exist_ok=True)

        with database.connection_context():
            corsika_settings = CorsikaSettings.get(id=corsika_run.corsika_settings_id)

        install_log_file = os.path.join(
            install_log_dir,
            f'corsika_{corsika_settings.version}_{corsika_settings.name}.log'
        )
        if os.path.isfile(install_log_file):
            raise ValueError(
                'CORSIKA installation tried before but failed, not trying again'
            )

        try:
            with open(install_log_file, 'w') as f:
                install_corsika(
                    corsika_dir,
                    corsika_settings.config_h,
                    corsika_settings.version,
                    corsika_settings.additional_files,
                    stdout=f, stderr=f,
                )
        except:
            shutil.rmtree(corsika_dir)
            raise

    with open(inputcard_file, 'w') as f:
        content = corsika_run.corsika_settings.format_input_card(corsika_run, output_file)
        f.write(content)

    env = os.environ.copy()
    env.update({
        'MOPRO_JOB_ID': str(corsika_run.id),
        'MOPRO_CORSIKA_DIR': corsika_dir,
        'MOPRO_INPUTCARD': inputcard_file,
        'MOPRO_OUTPUTDIR': output_dir,
        'MOPRO_OUTPUTFILE': output_file,
        'MOPRO_WALLTIME': str(corsika_run.walltime * 60),
        'MOPRO_SUBMITTER_HOST': submitter_host,
        'MOPRO_SUBMITTER_PORT': str(submitter_port),
        'MOPRO_LOGFILE': log_file,
        'FLUPRO': os.path.join(corsika_dir, 'fluka'),
    })

    if tmp_dir is not None:
        env['MOPRO_TMP_DIR'] = tmp_dir

    return dict(
        executable=script,
        env=env,
        stdout=log_file,
        job_name='mopro_corsika_{}'.format(corsika_run.id),
        walltime=corsika_run.walltime,
    )
