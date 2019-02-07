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

    host = os.environ['MOPRO_SUBMITTER_HOST']
    port = os.environ['MOPRO_SUBMITTER_PORT']
    socket.connect('tcp://{}:{}'.format(host, port))

    job_id = int(os.environ['MOPRO_JOB_ID'])

    def send_status_update(status, **kwargs):
        socket.send_pyobj({
            'program': 'corsika',
            'job_id': job_id,
            'status': status,
            **kwargs
        })

    send_status_update('running')
    socket.recv()

    output_dir = os.environ['MOPRO_OUTPUTDIR']
    output_file = os.environ['MOPRO_OUTPUTFILE']

    os.makedirs(output_dir, exist_ok=True)

    corsika_dir = os.environ['MOPRO_CORSIKA_DIR']
    corsika_exe = os.path.basename(glob(os.path.join(corsika_dir, 'run', 'corsika*'))[0])

    with open(os.environ['MOPRO_INPUTCARD'], 'rb') as f:
        inputcard = f.read()

    walltime = float(os.environ['MOPRO_WALLTIME'])
    log.info('Walltime = %.0f', walltime)

    job_name = 'fact_mopro_job_id_' + str(job_id) + '_'
    with tempfile.TemporaryDirectory(prefix=job_name) as tmp_dir:
        log.debug('Using tmp directory: {}'.format(tmp_dir))

        run_dir = os.path.join(tmp_dir, 'run')
        shutil.copytree(os.path.join(corsika_dir, 'run'), run_dir)
        timeout = walltime - (start_time - time.monotonic()) - 300
        try:
            sp.run(
                ['./' + corsika_exe],
                check=True,
                timeout=timeout,
                cwd=run_dir,
                input=inputcard,
            )

        except sp.CalledProcessError:
            send_status_update('failed')
            socket.recv()
            log.exception('Running CORSIKA failed')
            sys.exit(1)

        except sp.TimeoutExpired:
            send_status_update('walltime_exceeded')
            log.error('CORSIKA about to run into wall-time, terminating')
            socket.recv()
            sys.exit(1)
        except (KeyboardInterrupt, SystemExit):
            send_status_update('failed')
            log.error('Interrupted')
            socket.recv()
            sys.exit(1)

        try:
            log.info('Compressing file using zstd')
            result_file = os.path.join(output_dir, output_file + '.zst')
            sp.run([
                'zstd', '-5', '--rm',
                os.path.join(run_dir, output_file),
                '-o', result_file,
            ], check=True)
            log.info('Compressing done')
        except:
            log.exception('Compressing to output destination failed failed')
            send_status_update('failed')
            socket.recv()
            sys.exit(1)

    send_status_update(
        'success',
        result_file=result_file,
        duration=int(time.monotonic() - start_time),
    )
    socket.recv()


if __name__ == '__main__':
    main()
