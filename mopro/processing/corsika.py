import time
import subprocess as sp
import os
import logging
import tempfile
import sys
import shutil
from glob import glob
import zmq

start_time = time.monotonic()

context = zmq.Context()
socket = context.socket(zmq.REQ)

log = logging.getLogger('erna')
log.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
fmt = logging.Formatter(fmt='%(asctime)s [%(levelname)-8s] %(message)s')
handler.setFormatter(fmt)
logging.getLogger().addHandler(handler)


def main():
    log.info('CORSIKA executor started')

    host = os.environ['SUBMITTER_HOST']
    port = os.environ['SUBMITTER_PORT']
    socket.connect('tcp://{}:{}'.format(host, port))

    job_id = int(os.environ['SLURM_JOB_NAME'].replace('mopro_', ''))

    socket.send_pyobj({'job_id': job_id, 'status': 'running'})
    socket.recv()

    output_dir, filename = os.path.split(os.environ('OUTPUTFILE'))
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    corsika_dir = os.environ['CORSIKA_DIR']
    with open(os.environ['INPUTCARD'], 'rb') as f:
        inputcard = f.read()

    walltime = float(os.environ['WALLTIME'])
    log.info('Walltime = %.0f', walltime)

    job_name = 'fact_mopro_job_id_' + str(job_id) + '_'
    with tempfile.TemporaryDirectory(prefix=job_name) as tmp_dir:
        log.debug('Using tmp directory: {}'.format(tmp_dir))

        run_dir = os.path.join(tmp_dir, 'run')
        shutil.copytree(os.path.join(corsika_dir, 'run'), run_dir)
        timeout = walltime - (start_time - time.monotonic()) - 300
        corsika_exe = os.path.basename(glob(os.path.join('run_dir', 'corsika_*'))[0])
        try:
            sp.run(
                ['./' + corsika_exe],
                check=True,
                timeout=timeout,
                cwd=run_dir,
                input=inputcard,
            )

        except sp.CalledProcessError:
            socket.send_pyobj({'job_id': job_id, 'status': 'failed'})
            socket.recv()
            log.exception('Running CORSIKA failed')
            sys.exit(1)
        except sp.TimeoutExpired:
            socket.send_pyobj({'job_id': job_id, 'status': 'walltime_exceeded'})
            log.error('CORSIKA about to run into wall-time, terminating')
            socket.recv()
            sys.exit(1)

        try:
            log.info('Copying {} to {}'.format(output_file, output_dir))
            shutil.copy2(output_file, output_dir)
            output_file = os.path.join(output_dir, os.path.basename(output_file))
            log.info('Copy done')
        except:
            log.exception('Error copying outputfile')
            socket.send_pyobj({'job_id': job_id, 'status': 'failed'})
            socket.recv()
            sys.exit(1)

    try:
        process = sp.run(['md5sum', output_file], check=True, stdout=sp.PIPE)
        md5hash, _ = process.stdout.decode().split()
    except:
        log.exception('Error calculating md5sum')
        socket.send_pyobj({'job_id': job_id, 'status': 'failed'})
        socket.recv()
        sys.exit(1)

    socket.send_pyobj({
        'job_id': job_id,
        'status': 'success',
        'output_file': output_file,
        'md5hash': md5hash,
    })
    socket.recv()


if __name__ == '__main__':
    main()
