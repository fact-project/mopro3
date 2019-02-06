from threading import Thread, Event
import zmq
import logging
from retrying import retry
import peewee

from ..database import CorsikaRun, CeresRun, Status, database

log = logging.getLogger(__name__)


programs = {
    'corsika': CorsikaRun,
    'ceres': CeresRun,
}


def is_operational_error(exception):
    return isinstance(exception, peewee.OperationalError)


class JobMonitor(Thread):

    def __init__(self, port=12700):

        super().__init__()

        self.event = Event()
        self.port = port
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind('tcp://*:{}'.format(self.port))
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        log.info('JobMonitor running on port {}'.format(self.port))

    def run(self):
        while not self.event.is_set():
            events = self.poller.poll(timeout=1)
            for socket, n_messages in events:
                for i in range(n_messages):

                    status_update = socket.recv_pyobj()
                    socket.send_pyobj(True)
                    log.debug('Received status update: {}'.format(status_update))

                    self.update_job(status_update)

    @retry(retry_on_exception=is_operational_error)
    @database.connection_context()
    def update_job(self, update):
        model = programs[update.pop('program')]
        job_id = update.pop('job_id')

        update['status'] = Status.select().where(Status.name == update['status'])
        created = Status.select().where(Status.name == 'created')

        # the restriction on status != created
        # fixes a race condition where dying jobs
        # report failed status when the local cluster is shutdown
        return (
            model
            .update(**update)
            .where(model.id == job_id)
            .where(model.status != created)
        ).execute()

    def terminate(self):
        log.info('Monitor terminating')
        self.event.set()
