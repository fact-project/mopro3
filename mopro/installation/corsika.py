import subprocess as sp
import os
import click
import logging
from glob import glob
import shutil

from ..config import config
from .download import download_and_unpack


log = logging.getLogger(__name__)


CORSIKA_URL = 'ftp://ikp-ftp.ikp.kit.edu/'
USER = 'corsika'
FLUKA_URL = 'https://www.fluka.org/packages/fluka2011.2x-linux-gfor64bit-7.3-AA.tar.gz'


def download_corsika(path, version=76900, timeout=300):
    if os.path.exists(path):
        raise ValueError('CORSIKA download path already exists')

    version_dir = f'corsika-v{version // 1000:2d}0'
    basename = f'corsika-{version}'
    filename = basename + '.tar.gz'

    url = os.path.join(CORSIKA_URL, version_dir, filename)
    auth = f'{USER}:{config.corsika_password}'

    log.info(f'Downloading CORSIKA into {path}')
    download_and_unpack(url, path, auth=auth, timeout=timeout, strip=1)


def download_fluka(path, timeout=300):
    if os.path.exists(path):
        raise ValueError('FLUKA download path already exists')

    auth = f'{config.fluka_id}:{config.fluka_password}'

    log.info(f'Downloading FLUKA into {path}')
    download_and_unpack(FLUKA_URL, path, auth=auth, timeout=timeout)


def install_corsika(
    path,
    config_h,
    version=76900,
    additional_files=None,
    download_timeout=300,
    install_timeout=120,
):
    path = os.path.abspath(path)
    if os.path.exists(path):
        raise ValueError('CORSIKA install path already exists')

    env = os.environ.copy()
    env['F77'] = sp.check_output(['bash', '-c', 'which gfortran'])

    use_fluka = False
    for line in config_h.splitlines():
        if line.startswith('#define HAVE_FLUKA 1'):
            use_fluka = True

    download_corsika(path, version=version, timeout=download_timeout)
    if use_fluka:
        fluka_dir = os.path.join(path, 'fluka')
        download_fluka(fluka_dir, timeout=download_timeout)
        env['FLUPRO'] = fluka_dir

    with open(os.path.join(path, 'include', 'config.h'), 'w') as f:
        f.write(config_h)

    log.info(f'Starting CORSIKA installation in {path}')
    install = sp.Popen(
        ['./coconut', '-b'],
        cwd=path,
        stderr=sp.STDOUT, stdout=sp.PIPE,
        encoding='utf-8',
        env=env,
    )
    try:
        install.wait(timeout=install_timeout)
        if install.returncode != 0:
            log.error('CORSIKA installation failed')
            out, err = install.communicate()
            log.error(out)
            raise OSError(f'Failed to build CORSIKA:\n{out}')
    except sp.TimeoutExpired:
        log.error('CORSIKA installation timed out')

        install.terminate()
        install.wait()

        out, err = install.communicate()
        log.error(out)
        raise OSError(f'CORSIKA installation timed out:\n{out}')

    for f in glob(os.path.join(path, 'bernlohr', 'atmprof*')):
        name = os.path.basename(f)
        shutil.copy2(f, os.path.join(path, 'run', name))

    if additional_files is not None:
        sp.run(
            ['tar', 'xz', '-C', os.path.join(path, 'run')],
            input=additional_files,
            check=True,
        )


@click.command(name='install_corsika')
@click.argument('install_path', type=click.Path(exists=False))
@click.argument('config_h', type=click.Path(exists=True))
@click.option(
    '-v', '--version', type=click.INT, default=76900, show_default=True,
    help='CORSIKA Version as 5 digit integer',
)
def main(install_path, config_h, version):
    with open(config_h) as f:
        config_h = f.read()

    install_corsika(install_path, config_h, version=version)
