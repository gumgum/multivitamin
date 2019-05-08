import threading
from queue import Queue
import queue
import os
import string
import time
import glog as log

from rq import Queue as rQueue
from rq import Worker as rWorker
from redis import Redis
import shlex
from subprocess import Popen

import multiprocessing
from multiprocessing import Queue as mQueue


class ThreadWorker(threading.Thread):
    def __init__(self, q, func, timeout=1, on_kill=None, on_start=None):
        threading.Thread.__init__(self)
        log.info("New Worker Thread: {}".format(self.name))
        self._queue = q
        self._function = func
        self._timeout = timeout
        self._kill = False
        self._on_kill = on_kill
        self._on_start = on_start
        if self._on_start:
            self._on_start()

    def run(self):
        while not self._kill:
            try:
                log.debug("{} - Pulling from Queue".format(self.name))
                work = self._queue.get(timeout=self._timeout)
            except queue.Empty:
                log.debug("{} - Timed Out".format(self.name))
                continue
            if work:
                log.debug("{} - Running {}".format(self.name, self._function))
                log.debug("{} - Input: {}".format(self.name, work))
                self._function(work)

        if self._on_kill:
            self._on_kill()
        # print('Worker {} Dying'.format(os.getpid()))

    def kill(self):
        self._kill = True


class ThreadManager:
    def __init__(
        self, func, n=1, on_kill=None, on_start=None, timeout=1, max_queue_size=-1
    ):
        self.queue = Queue(max_queue_size)
        self._timeout = timeout
        self.workers = []
        for _ in range(n):
            self.workers.append(
                ThreadWorker(
                    self.queue,
                    func,
                    on_kill=on_kill,
                    on_start=on_start,
                    timeout=timeout,
                )
            )
            self.workers[-1].start()

    def _build_workers(self, func, n=1, on_kill=None, timeout=1, max_queue_size=-1):
        self.kill_workers(self.workers)
        self.workers = []
        self.queue = Queue(max_queue_size)
        for _ in range(n):
            self.workers.append(
                ThreadWorker(
                    self.queue,
                    func,
                    on_kill=on_kill,
                    on_start=on_start,
                    timeout=timeout,
                )
            )
            self.workers[-1].start()

    def kill_workers(self):
        for worker in self.workers:
            worker.kill()
            worker.join()

    def kill_workers_on_completion(self):
        while True:
            time.sleep(0.2)
            if self.queue.empty():
                break
        self.kill_workers()


class RedisWorker:
    def __init__(self, q, on_kill=None):
        self.queue = q
        self.on_kill = on_kill

    def run(self):
        start_worker_cmd = shlex.split("rq worker " + self.queue.name)
        self.process = Popen(start_worker_cmd)

    def kill(self):
        self.process.kill()
        if self.on_kill:
            self.on_kill()


class RedisManager:
    def __init__(self, n=1, on_kill=None, q_name="default-redis-queue"):
        redis_conn = Redis()
        self.queue = rQueue(q_name, connection=redis_conn)
        start_redis_server_cmd = shlex.split("redis-server")
        self.server = Popen(start_redis_server_cmd)
        self.workers = []
        for _ in range(n):
            self.workers.append(RedisWorker(self.queue, on_kill=on_kill))
            self.workers[-1].run()

    def kill_workers_on_completion(self):
        while True:
            time.sleep(0.2)
            if len(self.queue.job_ids) == 0:
                break
        self.kill_workers()

    def kill_workers(self):
        self.server.kill()
        for worker in self.workers:
            worker.kill()


class ProcessWorker(multiprocessing.Process):
    def __init__(self, q, func, timeout=1, on_kill=None, on_start=None):
        multiprocessing.Process.__init__(self)
        log.info("New Worker Process: {}".format(self.name))
        self._queue = q
        self._function = func
        self._timeout = timeout
        self._kill = False
        self._on_kill = on_kill
        self._on_start = on_start
        if self._on_start:
            self._on_start()

    def run(self):
        while not self._kill:
            try:
                log.debug("{} - Pulling from Queue".format(self.name))
                work = self._queue.get(timeout=self._timeout)
            except queue.Empty:
                log.debug("{} - Timed Out".format(self.name))
                continue
            if work:
                log.debug("{} - Running {}".format(self.name, self._function))
                log.debug("{} - Input: {}".format(self.name, work))
                self._function(work)

        if self._on_kill:
            self._on_kill()
        # print('Worker {} Dying'.format(os.getpid()))

    def kill(self):
        self._kill = True


class ProcessManager:
    def __init__(
        self, func, n=1, on_kill=None, on_start=None, timeout=1, max_queue_size=-1
    ):
        self.queue = mQueue(max_queue_size)
        self._timeout = timeout
        self.workers = []
        for _ in range(n):
            self.workers.append(
                ProcessWorker(
                    self.queue,
                    func,
                    on_kill=on_kill,
                    on_start=on_start,
                    timeout=timeout,
                )
            )
            self.workers[-1].start()

    def _build_workers(self, func, n=1, on_kill=None, timeout=1, max_queue_size=-1):
        self.kill_workers(self.workers)
        self.workers = []
        self.queue = mQueue(max_queue_size)
        for _ in range(n):
            self.workers.append(
                ProcessWorker(
                    self.queue,
                    func,
                    on_kill=on_kill,
                    on_start=on_start,
                    timeout=timeout,
                )
            )
            self.workers[-1].start()

    def kill_workers(self):
        for worker in self.workers:
            worker.kill()
            worker.join()

    def kill_workers_on_completion(self):
        while True:
            time.sleep(0.2)
            if self.queue.empty():
                break
        self.kill_workers()


def WorkerManager(
    func=None,
    n=1,
    on_kill=None,
    on_start=None,
    timeout=1,
    max_queue_size=-1,
    parallelization="thread",
    q_name="default-redis-queue",
):
    if parallelization == "thread":
        assert callable(func)
        return ThreadManager(
            func,
            n=n,
            on_kill=on_kill,
            on_start=on_start,
            timeout=timeout,
            max_queue_size=max_queue_size,
        )
    if parallelization == "redis":
        return RedisManager(n=n, on_kill=on_kill, q_name=q_name)
    if parallelization == "process":
        assert callable(func)
        return ProcessManager(
            func,
            n=n,
            on_kill=on_kill,
            on_start=on_start,
            timeout=timeout,
            max_queue_size=max_queue_size,
        )


if __name__ == "__main__":
    ###########
    # EXAMPLE #
    ###########
    q = Queue()
    workers = []
    for _ in range(2):
        workers.append(ThreadWorker(q, print))
        workers[-1].start()
    for char in string.printable:
        q.put(char)
    while True:
        time.sleep(0.2)
        if q.empty():
            break
    for worker in workers:
        worker.kill()
        worker.join()
