import subprocess as sp
import tempfile
import os
from multiprocessing import cpu_count
import logging
import sysconfig
import sys
import click


from .download import download_and_unpack

log = logging.getLogger(__name__)
ROOT5_URL = 'https://github.com/root-project/root/archive/v5-34-00-patches.tar.gz'


def install_root(path, cores=cpu_count(), stdout=None, stderr=None):
    path = os.path.abspath(path)

    if os.path.exists(path):
        raise ValueError('ROOT installation path already exists')

    with tempfile.TemporaryDirectory(prefix='mopro_root_') as tmpdir:

        log.info(f'Downloading root from {ROOT5_URL}')
        source_dir = os.path.join(tmpdir, 'source')
        download_and_unpack(ROOT5_URL, source_dir, strip=1)

        build_dir = os.path.join(tmpdir, 'build')
        os.makedirs(build_dir)

        # this fixes issues that arise when conda is used
        # it basically makes sure, root is build against systemlibs
        # not anaconda libs
        env = os.environ.copy()
        paths = env['PATH'].split(':')
        env['PATH'] = ':'.join(filter(
            lambda p: not p.startswith(sys.base_prefix),
            paths
        ))

        cmake_call = [
            'cmake',
            f'-DCMAKE_INSTALL_PREFIX={path}',
            '-DCMAKE_CXX_STANDARD=11',
            '-Dbuiltin_zlib=ON',
            '-Dmathmore=ON',
            '-Dminuit2=ON',
            '-Dasimage=ON',
        ]

        # common keyword args for each run call
        kwargs = dict(check=True, stdout=stdout, stderr=stderr, cwd=build_dir)

        log.info(f'Running cmake for  ROOT in {tmpdir}')
        # first run without python in PATH
        sp.run(cmake_call + [source_dir], env=env, **kwargs)

        # second run with python config
        library = os.path.join(
            sysconfig.get_config_var('LIBDIR'),
            sysconfig.get_config_var('LDLIBRARY')
        )
        cmake_call.extend([
            '-DPYTHON_EXECUTABLE={}'.format(sys.executable),
            '-DPYTHON_INCLUDE_DIR={}'.format(sysconfig.get_path('include')),
            '-DPYTHON_LIBRARY={}'.format(library),
        ])

        sp.run(cmake_call + [source_dir], env=env, **kwargs)
        log.info(f'Running make for  ROOT in {tmpdir}')
        sp.run(['make', f'-j{cores:d}'], env=env, **kwargs)

        log.info(f'Running make install for  ROOT in {tmpdir}')
        sp.run(['make', f'-j{cores:d}', 'install'], env=env, **kwargs)


@click.command(name='install_root_5')
@click.argument('install_path')
@click.option(
    '-j', '--n-jobs',
    default=cpu_count(), type=click.INT, show_default=True,
    help='Number of cores to use, default=all',
)
def main(install_path, n_jobs):
    install_root(install_path, cores=n_jobs)


if __name__ == '__main__':
    main()
