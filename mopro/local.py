from threading import Thread, Event
import subprocess as sp
from queue import Queue, Empty
import logging
from collections import namedtuple


Job = namedtuple(
    'ProcessData',
    ['executable', 'args', 'env', 'stdout', 'stderr', 'job_name', 'walltime']
)

log = logging.getLogger(__name__)


class LocalCluster(Thread):
    def __init__(self, max_workers):
        super().__init__()
        self.max_workers = max_workers
        self.event = Event()
        self.queue = Queue()
        self.running_jobs = []

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

        self.queue.put(Job(executable, args, env, stdout, stderr, job_name, walltime))

    def terminate(self):
        self.event.set()
        for job in self.running_jobs:
            if job.poll() is None:
                job.terminate()

    def run(self):
        while not self.event.is_set():
            # remove finished jobs from the list of running jobs
            for job in self.running_jobs[:]:
                if job.poll() is not None:
                    self.running_jobs.remove(job)

            # check if we can start a new job
            if len(self.running_jobs) < self.max_workers:
                try:
                    job = self.queue.get(timeout=0.1)
                except Empty:
                    continue

            # start a new job
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
            self.running_jobs.append(p)

    def get_running_jobs(self):
        return len(self.running_jobs)

    def get_queued_jobs(self):
        return len(self.queue)
