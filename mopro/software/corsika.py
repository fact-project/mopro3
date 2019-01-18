import subprocess as sp
import os
from ..config import Config
from .download import download_and_unpack
import logging


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
    auth = f'{USER}:{Config.corsika_password}'

    log.info(f'Downloading CORSIKA into {path}')
    download_and_unpack(url, path, auth=auth, timeout=timeout, strip=1)


def download_fluka(path, timeout=300):
    if os.path.exists(path):
        raise ValueError('FLUKA download path already exists')

    auth = f'{Config.fluka_id}:{Config.fluka_password}'

    log.info(f'Downloading FLUKA into {path}')
    download_and_unpack(FLUKA_URL, path, auth=auth, timeout=timeout)


def install_corsika(
    path,
    config_h,
    version=76900,
    download_timeout=300,
    install_timeout=120,
):
    if os.path.exists(path):
        raise ValueError('CORSIKA install path already exists')

    download_corsika(path, version=version, timeout=download_timeout)

    use_fluka = False
    for line in config_h:
        if line.startswith('#define HAVE_FLUKA 1'):
            use_fluka = True

    if use_fluka:
        fluka_dir = os.path.join(path, 'fluka')
        download_fluka(fluka_dir, timeout=download_timeout)
        os.environ['FLUPRO'] = fluka_dir

    with open(os.path.join(path, 'include', 'config.h'), 'w') as f:
        f.write(config_h)

    log.info(f'Starting CORSIKA installation in {path}')
    install = sp.Popen(
        ['./coconut', '-b'],
        cwd=path,
        stderr=sp.STDOUT, stdout=sp.PIPE,
        encoding='utf-8'
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
