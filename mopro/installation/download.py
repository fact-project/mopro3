import subprocess as sp
import os


def download_and_unpack(url, path, auth=None, strip=0, timeout=300):
    os.makedirs(path)

    call = ['curl', '--silent', '-L', '--show-error', '--fail']
    if auth is not None:
        call.extend(['--user', auth])
    call.append(url)

    curl = sp.Popen(
        call,
        stdout=sp.PIPE,
        stderr=sp.PIPE,
    )
    tar = sp.Popen(
        [
            'tar',
            'xz',               # uncompress
            '-C', path,         # output into `path`
            f'--strip={strip}'  # remove `strip` layers of toplevel directories
        ],
        stdin=curl.stdout,
        stdout=sp.PIPE,
        stderr=sp.PIPE
    )

    try:
        curl.wait(timeout=timeout)
        tar.wait(timeout=timeout)
    except sp.TimeoutExpired:
        curl.kill()
        tar.kill()

    if curl.returncode != 0:
        raise IOError(f'Error downloading {url}: {curl.stderr.read().decode()}')

    if tar.returncode != 0:
        raise IOError(f'Error untarring {url}: {tar.stderr.read().decode()}')
