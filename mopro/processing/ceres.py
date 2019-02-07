import os
import logging
from pkg_resources import resource_filename

from ..database import database, CeresSettings
from ..corsika_utils import primary_id_to_name
from ..installation import install_root, install_mars

log = logging.getLogger(__name__)


def build_directory_name(ceres_run):
    ceres_settings = ceres_run.ceres_settings
    corsika_run = ceres_run.corsika_run
    mode = build_mode_string(ceres_run)
    return os.path.join(
        'ceres',
        f'r{ceres_settings.revision}',
        f'{ceres_settings.name}',
        corsika_run.corsika_settings.name,
        primary_id_to_name(corsika_run.primary_particle),
        mode,
        f'{corsika_run.id // 1000:08d}',
    )


def build_mode_string(ceres_run):
    corsika_run = ceres_run.corsika_run
    if ceres_run.diffuse or corsika_run.viewcone > 0:
        mode = f'diffuse_{ceres_run.off_target_distance:.0f}d'
    else:
        if ceres_run.off_target_distance > 0:
            mode = f'wobble_{ceres_run.off_target_distance:.1f}d'
        else:
            mode = 'on'
    return mode


def build_basename(ceres_run):
    corsika_run = ceres_run.corsika_run
    ceres_settings = ceres_run.ceres_settings
    mode = build_mode_string(ceres_run)
    return 'ceres_{primary}_{mode}_run_{run:08d}_az{min_az:03.0f}-{max_az:03.0f}_zd{min_zd:02.0f}-{max_zd:02.0f}'.format(
        name=ceres_settings.name,
        primary=primary_id_to_name(corsika_run.primary_particle),
        mode=mode,
        run=corsika_run.id,
        min_az=corsika_run.azimuth_min,
        max_az=corsika_run.azimuth_max,
        min_zd=corsika_run.zenith_min,
        max_zd=corsika_run.zenith_max,
    )


def prepare_ceres_job(
    ceres_run,
    mopro_directory,
    submitter_host,
    submitter_port,
):
    ceres_settings = ceres_run.ceres_settings
    corsika_run = ceres_run.corsika_run

    script = resource_filename('mopro', 'resources/run_ceres.sh')
    directory = build_directory_name(ceres_run)
    basename = build_basename(ceres_run)

    output_dir = os.path.join(mopro_directory, directory)
    log_dir = os.path.join(mopro_directory, 'logs', directory)
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    log_file = os.path.join(log_dir, basename + '.log')

    root_dir = os.path.join(mopro_directory, 'software', 'root')
    if not os.path.exists(root_dir):
        install_root(root_dir)

    mars_dir = os.path.join(
        mopro_directory, 'software', 'mars', str(ceres_settings.revision),
    )
    if not os.path.exists(mars_dir):
        install_mars(mars_dir, root_path=root_dir, revision=ceres_settings.revision)

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

    return dict(
        executable=script,
        env=env,
        stdout=log_file,
        job_name='mopro_ceres_{}'.format(ceres_run.id),
        walltime=ceres_run.walltime,
    )
