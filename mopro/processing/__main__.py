import click
import logging
import time
import os

from ..database import initialize_database
from .monitor import JobMonitor
from .submitter import JobSubmitter
from ..config import config
from ..slurm import SlurmCluster
from ..local import LocalCluster

log = logging.getLogger(__name__)


@click.command()
@click.option(
    '--config-file', '-c',
    type=click.Path(dir_okay=False, exists=True),
    help='Config file, if not given, $HOME/mopro.yaml and ./mopro.yaml will be tried'
)
@click.option(
    '--verbose', '-v', help='Set log level of "erna" to debug', is_flag=True,
)
def main(config_file, verbose):

    if config_file is not None:
        config.load_yaml(config_file)

    logging.getLogger('mopro').setLevel(logging.INFO)
    if verbose:
        logging.getLogger('mopro').setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler()
    os.makedirs(config.mopro_directory, exist_ok=True)
    file_handler = logging.FileHandler(os.path.join(config.mopro_directory, 'submitter.log'))
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s|%(levelname)s|%(message)s'
    )

    for handler in (stream_handler, file_handler):
        handler.setFormatter(formatter)
        logging.getLogger().addHandler(handler)

    log.info('Initialising database')
    initialize_database()

    if config.submitter.mode == 'local':
        cluster = LocalCluster(config.local.cores)
    else:
        cluster = SlurmCluster(
            mail_address=config.cluster.mail_address,
            mail_settings=config.cluster.mail_settings,
            memory=config.cluster.memory,
            partitions=config.partitions,
        )

    job_monitor = JobMonitor(port=config.submitter.port)
    job_submitter = JobSubmitter(
        mopro_directory=config.mopro_directory,
        interval=config.submitter.interval,
        max_queued_jobs=config.submitter.max_queued_jobs,
        host=config.submitter.host,
        port=config.submitter.port,
        cluster=cluster,
    )

    log.info('Starting main loop')
    try:
        job_monitor.start()
        job_submitter.start()
        while True:
            time.sleep(10)

    except (KeyboardInterrupt, SystemExit):
        log.info('Shutting done')
        job_monitor.terminate()
        job_submitter.terminate()
        job_submitter.join()


if __name__ == '__main__':
    main()
