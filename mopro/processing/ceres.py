import os
import logging
from pkg_resources import resource_filename
import shutil

from ..database import database, CeresSettings
from ..installation import install_root, install_mars

log = logging.getLogger(__name__)


def prepare_ceres_job(
    ceres_run,
    mopro_directory,
    submitter_host,
    submitter_port,
    tmp_dir=None,
):
    ceres_settings = ceres_run.ceres_settings
    corsika_run = ceres_run.corsika_run

    script = resource_filename('mopro', 'resources/run_ceres.sh')
    directory = ceres_run.directory_name
    basename = ceres_run.basename

    output_dir = os.path.join(mopro_directory, directory)
    log_dir = os.path.join(mopro_directory, 'logs', directory)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, basename + '.log')

    root_dir = os.path.join(mopro_directory, 'software', 'root')
    install_log_dir = os.path.join(mopro_directory, 'logs', 'installation')
    os.makedirs(install_log_dir, exist_ok=True)

    if not os.path.exists(root_dir):
        install_logfile = os.path.join(install_log_dir, 'root.log')
        if os.path.isfile(install_logfile):
            raise ValueError(
                'ROOT installation previously attempted but failed, not trying again'
            )
        with open(install_logfile, 'w') as f:
            install_root(root_dir, stdout=f, stderr=f)

    mars_dir = os.path.join(
        mopro_directory, 'software', 'mars', str(ceres_settings.revision),
    )
    if not os.path.exists(mars_dir):
        install_logfile = os.path.join(
            install_log_dir,
            f'mars_r{ceres_settings.revision}.log'
        )
        if os.path.isfile(install_logfile):
            raise ValueError(
                'MARS installation previously attempted but failed, not trying again'
            )
        try:
            with open(install_logfile, 'w') as f:
                install_mars(
                    mars_dir,
                    root_path=root_dir,
                    revision=ceres_settings.revision,
                    stdout=f,
                    stderr=f,
                )
        except:
            # clean up after failed installation
            shutil.rmtree(mars_dir)
            raise

    resource_dir = os.path.join(
        mopro_directory,
        'ceres_settings',
        ceres_settings.name,
        f'r{ceres_settings.revision}',
    )
    if not os.path.exists(resource_dir):
        with database.connection_context():
            ceres_settings = CeresSettings.get(id=ceres_settings.id)
        log.info(f'Writing ceres resources into {resource_dir}')
        ceres_settings.write_resources(resource_dir)

    rc_file = ceres_settings.rc_path(ceres_run, resource_dir)
    if not os.path.isfile(rc_file):
        ceres_settings = CeresSettings.get(id=ceres_settings.id)
        log.info(f'Writing ceres rc to {rc_file}')
        ceres_settings.write_rc(ceres_run, resource_dir)

    env = os.environ.copy()
    env['PATH'] = ':'.join([os.path.join(root_dir, 'bin'), mars_dir, env['PATH']])
    ld_library_paths = [os.path.join(root_dir, 'lib'), mars_dir]
    if env.get('LD_LIBRARY_PATH') is not None:
        ld_library_paths.append(env['LD_LIBRARY_PATH'])
    env['LD_LIBRARY_PATH'] = ':'.join(ld_library_paths)

    env.update({
        'MARSSYS': mars_dir,
        'MOPRO_JOB_ID': str(ceres_run.id),
        'MOPRO_CERES_RC': rc_file,
        'MOPRO_CORSIKA_RUN': str(corsika_run.id),
        'MOPRO_INPUTFILE': corsika_run.result_file,
        'MOPRO_OUTPUTDIR': output_dir,
        'MOPRO_OUTPUTBASENAME': basename,
        'MOPRO_WALLTIME': str(ceres_run.walltime * 60),
        'MOPRO_SUBMITTER_HOST': submitter_host,
        'MOPRO_SUBMITTER_PORT': str(submitter_port),
    })

    if tmp_dir is not None:
        env['MOPRO_TMP_DIR'] = tmp_dir

    return dict(
        executable=script,
        env=env,
        stdout=log_file,
        job_name='mopro_ceres_{}'.format(ceres_run.id),
        walltime=ceres_run.walltime,
    )
