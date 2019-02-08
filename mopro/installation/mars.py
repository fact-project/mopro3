import subprocess as sp
import os
from multiprocessing import cpu_count
import click


URL = 'https://trac.fact-project.org/svn/trunk/Mars'


def install_mars(
        path,
        root_path,
        revision=19425,
        cores=cpu_count(),
        stdout=None,
        stderr=None,
):
    sp.run(
        ['svn', 'checkout', '-q', '-r', str(revision), URL, path],
        check=True, stdout=stdout, stderr=stderr,
    )

    env = os.environ.copy()

    for k, subdir in zip(['PATH', 'LD_LIBRARY_PATH'], ['bin', 'lib']):
        env[k] = os.path.join(root_path, subdir) + ':' + os.environ[k]

    sp.run(['make', f'-j{cores}'], cwd=path, env=env, stdout=stdout, stderr=stderr)


@click.command(name='install_mars')
@click.argument('install_path')
@click.argument('root_path')
@click.option(
    '-j', '--n-jobs',
    default=cpu_count(), type=click.INT, show_default=True,
    help='Number of cores to use, default=all',
)
@click.option(
    '-r', '--revision',
    default=19425, type=click.INT, show_default=True,
    help='Number of cores to use, default=all',
)
def main(install_path, root_path, n_jobs, revision):
    install_mars(install_path, root_path, revision=revision, cores=n_jobs)


if __name__ == '__main__':
    main()
