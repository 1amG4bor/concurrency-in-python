import math
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from enum import Enum
from queue import Queue
from time import sleep
from typing import Callable, List

from multi_tasking.helper.prime_helper import prim_task, time_it
from multi_tasking.model import TaskRunner
from multi_tasking.model.const import TASK_PREPARATION_TIME


# https://realpython.com/intro-to-python-threading/
# .Thread(name, target, args, kwargs, daemon) - create a thread (start, join)
# Multi-thread
#   - manual: one or more Thread()
#   - ThreadPoolExecutor: .submit | .map
#   - ProcessPoolExecutor => "uses the multiprocessing module"
# Race conditions:  - .Lock (.acquire()/.release()) /mutex/
#                   - (Bounded)Semaphore (counter to limit threads to access resources)
# Timer(sec: float) - schedule a function to be called after a certain amount of time
# Barrier(n) - keep a fixed number of threads in sync. (allow a pool of threads to initialize themselves)
#       Each thread calls .wait() on the Barrier. They all will remain blocked
#       until the specified number of threads are waiting, and then they are all released at the same time.


class MultiThreadRunnerType(Enum):
    SIMPLE = "manually managed Threads"
    LOCKED_LIST = "manually managed locking Threads working from common List"
    THREAD_POOL = "ThreadPoolExecutor"


class MultiThreadTaskRunner(TaskRunner):
    def __init__(self, task: Callable, max_workers: int, runner: MultiThreadRunnerType):
        super().__init__(task, max_workers)
        self._task = task
        self.__max_workers = max_workers
        self.__runner_type: MultiThreadRunnerType = runner

    def execute(self, task_queue: Queue):
        """The entrypoint and separator of different task execution implementations."""
        print(f"\nProcessing with '{self.__runner_type.value}'... ({self.__max_workers} threads)")
        match self.__runner_type:
            case MultiThreadRunnerType.SIMPLE:
                self.run_simple(task_queue)
            case MultiThreadRunnerType.LOCKED_LIST:
                self.run_with_list_and_lock(task_queue)
            case MultiThreadRunnerType.THREAD_POOL:
                self.run_in_pool(task_queue)

        return self._result

    def run_simple(self, task_queue: Queue):
        """Execute the task with a group of threads"""
        working_threads = list()
        for worker in self._workers:
            thread = threading.Thread(
                target=worker.process,
                args=(task_queue, self._result[worker.name]))
            working_threads.append(thread)
            thread.start()

        for thread in working_threads:
            thread.join()

    def run_with_list_and_lock(self, task_queue):
        task_list = self._get_chunks(task_queue, 1)[0]
        lock = threading.Lock()
        task_provider = self._task_provider(task_list, lock)

        workers_size = self.__max_workers
        working_threads = list()
        for worker_name in [f'Worker-{n}' for n in range(1, workers_size + 1)]:
            thread = threading.Thread(
                name=worker_name,
                target=self.__processing_with_lock,
                args=(task_provider, self._result[worker_name]))
            working_threads.append(thread)
            thread.start()

        for thread in working_threads:
            thread.join()

    def run_in_pool(self, task_queue: Queue):
        """Execute the task with a pool of managed threads."""
        self._result = dict({})
        pool_size = self.__max_workers
        chunks = self._get_chunks(task_queue, pool_size)

        with ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix='Worker') as executor:
            # Schedules a fn(*args, **kwargs) and returns a Future object representing the execution of the callable.
            # future = executor.submit(func)
            # future.result()
            result = executor.map(self.__pool_processing, chunks)
            for name, primes in result:
                self._result[name] = primes

    @staticmethod
    def _get_chunks(task_queue: Queue, num_of_chunk: int):
        tasks = [task_queue.get() for _ in range(task_queue.qsize())]
        total = len(tasks)
        chunk_size = math.ceil(total / num_of_chunk)
        chunks = [tasks[i:i + chunk_size] for i in range(0, total, chunk_size)]
        return chunks

    @staticmethod
    def _task_provider(task_list: List[int], lock: threading.Lock):
        def provider():
            if len(task_list) < 1:
                return None
            while True:
                if lock.acquire() is True:
                    task = task_list.pop(0)
                    lock.release()
                    return task
                else:
                    sleep(0.01)
        return provider

    def __pool_processing(self, task_queue):
        name = threading.current_thread().name
        print(f'{name}: start working.. (PID:{os.getpid()})')
        result = set({})
        for item in task_queue:
            sleep(TASK_PREPARATION_TIME)  # Prepare for the job
            out = self._task(item)
            if out:
                result.add(out)
        return name, result

    def __processing_with_lock(self, task_provider, result):
        name = threading.current_thread().name
        print(f'{name}: start working.. (PID:{os.getpid()})')
        while True:
            sleep(TASK_PREPARATION_TIME)  # Prepare for the job
            item = task_provider()
            if not item:
                break
            # print(f'{name} checking: {item}')
            output = self._task(item)
            if output:
                result.add(output)
        return name, result


if __name__ == '__main__':
    @time_it
    def multithread_calculate(limit: int, runner: MultiThreadRunnerType, workers: int = 3):
        task_queue = Queue()
        for n in range(1, limit + 1):
            task_queue.put(n)

        task_runner = MultiThreadTaskRunner(prim_task, workers, runner)
        result = task_runner.execute(task_queue)
        print(result)


    up_to = 100
    print("GENERATED DELAY:", up_to * 0.01, 'sec')
    multithread_calculate(up_to, MultiThreadRunnerType.SIMPLE)
    multithread_calculate(up_to, MultiThreadRunnerType.LOCKED_LIST)
    multithread_calculate(up_to, MultiThreadRunnerType.THREAD_POOL, 2)
    multithread_calculate(up_to, MultiThreadRunnerType.THREAD_POOL, 3)
    multithread_calculate(up_to, MultiThreadRunnerType.THREAD_POOL, 5)
