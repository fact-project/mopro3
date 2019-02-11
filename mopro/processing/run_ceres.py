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
    log.info('CERES executor started')

    host = os.environ['MOPRO_SUBMITTER_HOST']
    port = os.environ['MOPRO_SUBMITTER_PORT']
    socket.connect('tcp://{}:{}'.format(host, port))

    job_id = int(os.environ['MOPRO_JOB_ID'])

    def send_status_update(status, **kwargs):
        socket.send_pyobj({
            'program': 'ceres',
            'job_id': job_id,
            'status': status,
            **kwargs
        })

    send_status_update('running')
    socket.recv()

    output_dir = os.environ['MOPRO_OUTPUTDIR']
    output_base = os.path.join(output_dir, os.environ['MOPRO_OUTPUTBASENAME'])
    input_file = os.environ['MOPRO_INPUTFILE']
    rc_file = os.environ['MOPRO_CERES_RC']
    corsika_run = int(os.environ['MOPRO_CORSIKA_RUN'])

    os.makedirs(output_dir, exist_ok=True)

    walltime = float(os.environ['MOPRO_WALLTIME'])
    log.info('Walltime = %.0f', walltime)

    job_name = 'fact_mopro_job_id_' + str(job_id) + '_'
    with tempfile.TemporaryDirectory(prefix=job_name) as tmp_dir:
        log.debug('Using tmp directory: {}'.format(tmp_dir))

        cerfile = f'cer{corsika_run:08d}'
        tmp_input_file = os.path.join(tmp_dir, cerfile)

        try:
            sp.run(['zstd', '-d', input_file, '-o', tmp_input_file], check=True)
        except sp.CalledProcessError:
            send_status_update('failed')
            socket.recv()
            log.exception('Failed to decompress input file')
            sys.exit(1)

        timeout = walltime - (start_time - time.monotonic()) - 300
        try:
            cmd = [
                'ceres',
                '-b',
                f'--config={rc_file}',
                f'--out={tmp_dir}',
                '--fits',
                f'--run-number={corsika_run}',
                cerfile,
            ]
            log.info('Calling ceres using "{}"'.format(' '.join(cmd)))
            sp.run(cmd, check=True, timeout=timeout, cwd=tmp_dir)
        except sp.CalledProcessError:
            send_status_update('failed')
            socket.recv()
            log.exception('Running CERES failed')
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
            events_file = glob(os.path.join(tmp_dir, '*Events.fits'))[0]
            run_file = glob(os.path.join(tmp_dir, '*RunHeaders.fits'))[0]
            events_gz_file = f'{output_base}_Events.fits.gz'
            run_gz_file = f'{output_base}_RunHeaders.fits.gz'

            log.info(f'Gzipping {events_file} to {events_gz_file}')
            with open(events_gz_file, 'wb') as f:
                sp.run(['gzip', '--to-stdout', events_file], stdout=f, check=True)

            log.info(f'Gzipping {run_file} to {run_gz_file}')
            with open(run_gz_file, 'wb') as f:
                sp.run(['gzip', '--to-stdout', run_file], stdout=f, check=True)

            log.info('gzipping done')
        except:
            log.exception('Error gzipping outputfiles to target destination')
            send_status_update('failed')
            socket.recv()
            sys.exit(1)

    send_status_update(
        'success',
        result_events_file=output_base + '_Events.fits.gz',
        result_runheader_file=output_base + '_RunHeaders.fits.gz',
        duration=int(time.monotonic() - start_time),
    )
    socket.recv()


if __name__ == '__main__':
    main()
