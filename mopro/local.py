from threading import Thread, Event
import subprocess as sp
import logging
from collections import namedtuple, deque
from .cluster import Cluster


Job = namedtuple(
    'ProcessData',
    ['executable', 'args', 'env', 'stdout', 'stderr', 'job_name', 'walltime']
)


class LocalCluster(Cluster, Thread):
    log = logging.getLogger(__name__)

    def __init__(self, max_workers):
        super().__init__()
        self.max_workers = max_workers
        self.event = Event()
        self.queue = deque()
        self.running_jobs = {}

    def submit_job(
        self,
        executable,
        *args,
        env=None,
        stdout=None,
        stderr=None,
        job_name=None,
        walltime=None,
    ):
        if args is not None:
            if not all(isinstance(arg, (bytes, str)) for arg in args):
                raise ValueError('bytes or string expected')

        if self.event.is_set():
            raise ValueError('Cluster was already terminated')

        self.queue.append(Job(executable, args, env, stdout, stderr, job_name, walltime))

    def terminate(self):
        self.log.info('Local cluster terminating')
        self.event.set()
        super().terminate()
        self.join()

    def kill_job(self, job_name):
        job = self.running_jobs.get(job_name)
        if job is not None:
            job.kill()

    def cancel_job(self, job_name):
        self.queue = deque([
            job for job in self.queue
            if job.job_name != job_name
        ])

    def start(self):
        self.log.info(f'Starting local cluster with {self.max_workers} workers max')
        super().start()

    def run(self):
        while not self.event.is_set():
            # remove finished jobs from the list of running jobs
            self.running_jobs = {
                name: job for name, job in self.running_jobs.items()
                if job.poll() is None
            }

            # check if we can start a new job
            if self.n_running < self.max_workers and self.n_queued > 0:
                job = self.queue.popleft()
                self.start_job(job)
            else:
                self.event.wait(1)

    def start_job(self, job):
        cmd = [job.executable]
        if job.args is not None:
            cmd.extend(job.args)

        if job.stdout is not None:
            stdout = open(job.stdout, 'w')
        else:
            stdout = sp.PIPE

        if job.stderr is not None:
            stderr = open(job.stderr, 'w')
        else:
            stderr = sp.STDOUT

        p = sp.Popen(cmd, env=job.env, stdout=stdout, stderr=stderr)
        self.running_jobs[job.job_name] = p

    @property
    def n_running(self):
        return len(self.running_jobs)

    @property
    def n_queued(self):
        return len(self.queue)

    def get_running_jobs(self):
        return list(self.running_jobs.keys())

    def get_queued_jobs(self):
        return [j.job_name for j in self.queue]
