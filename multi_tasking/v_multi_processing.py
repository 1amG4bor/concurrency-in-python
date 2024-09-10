import math
import os
from concurrent.futures import ProcessPoolExecutor, wait, ALL_COMPLETED, Future
from enum import Enum
from multiprocessing import Manager, Process, current_process, Queue
from time import sleep
from typing import Callable, Dict, List

from multi_tasking.helper.prime_helper import prim_task, time_it
from multi_tasking.model import TaskRunner, Worker
from multi_tasking.model.const import TASK_PREPARATION_TIME


class MultiProcessRunnerType(Enum):
    SIMPLE = "manually managed Processes"
    SHARED = "manually managed Processes with shared queue"
    PROCESS_POOL = "ProcessPoolExecutor"
    CHUNKS_IN_POOL = "ProcessPoolExecutor working on chunks"


class MultiProcessWorker(Worker):
    def __init__(self, name: str, task: Callable, delay: int = TASK_PREPARATION_TIME):
        super().__init__(name, task, delay)

    def process(self, task_list, manager_dict):
        print(self.name, 'start working..', f'(PID:{os.getpid()})')
        result = self.__processing_list(task_list)
        manager_dict[self.name] = result

    def process_with_shared_resource(self, shared_queue: Queue, result_dict: Dict):
        name = current_process().name
        print(f'{name}: start working.. (PID:{os.getpid()})')
        result = set({})
        while not shared_queue.empty():
            sleep(self._forced_delay)  # Forced delay to indicate preparation for the job
            try:
                item = shared_queue.get(block=False)
            except Exception:
                continue

            out = self._task(item)
            if out:
                result.add(out)

        result_dict[name] = result

    def process_with_chunks(self, task_list):
        print(self.name, 'start working..', f'(PID:{os.getpid()})')
        result = self.__processing_list(task_list)
        return self.name, result

    def __processing_list(self, task_list):
        result = set({})
        for item in task_list:
            sleep(self._forced_delay)  # Forced delay to indicate preparation for the job
            output = self._task(item)
            if output:
                result.add(output)
        return result


class MultiProcessTaskRunner(TaskRunner):
    def __init__(self, task: Callable, max_workers: int, runner: MultiProcessRunnerType, forced_delay: int = None):
        super().__init__(task, max_workers, MultiProcessWorker, forced_delay)
        self._task = task
        self.__max_workers = max_workers
        self.__runner_type: MultiProcessRunnerType = runner

    def execute(self, task_range: range):
        """The entrypoint and separator of different task execution implementations."""
        print(f"\nProcessing with '{self.__runner_type.value}'... ({self.__max_workers} processes)")
        match self.__runner_type:
            case MultiProcessRunnerType.SIMPLE:
                self.run_simple(task_range)
            case MultiProcessRunnerType.SHARED:
                self.run_simple_shared(task_range)
            case MultiProcessRunnerType.PROCESS_POOL:
                self.run_in_pool(task_range)
            case MultiProcessRunnerType.CHUNKS_IN_POOL:
                self.run_in_pool_with_chunks(task_range)

        return self._result

    def run_simple(self, task_range: range):
        """Execute the task with a group of processes"""
        num_of_workers = self.__max_workers
        chunks = self._get_chunks(task_range, num_of_workers)

        manager = Manager()
        return_dict = manager.dict()

        processes = list()
        for idx, worker in enumerate(self._workers):
            process = Process(
                target=worker.process,
                args=(chunks[idx], return_dict))
            processes.append(process)
            process.start()

        for process in processes:
            process.join()

        self._result = dict(return_dict)

    def run_simple_shared(self, task_range: range):
        """Execute the task with a group of processes with shared queue"""

        with Manager() as manager:
            shared_queue = manager.Queue()
            for item in task_range:
                shared_queue.put(item)
            result_dict = manager.dict()

            processes = list()
            worker: MultiProcessWorker
            for worker in self._workers:
                process = Process(name=worker.name,
                                  target=worker.process_with_shared_resource,
                                  args=(shared_queue, result_dict))
                process.start()
                processes.append(process)

            for process in processes:
                process.join()
            self._result = dict(result_dict)

    def run_in_pool(self, task_range: range):
        """Execute the task with a pool of managed threads."""
        with Manager() as manager:
            shared_queue = manager.Queue()
            for item in task_range:
                shared_queue.put(item)
            result_dict = manager.dict()

            with ProcessPoolExecutor(max_workers=self.__max_workers) as executor:
                futures: List[Future] = list()
                worker: MultiProcessWorker
                for worker in self._workers:
                    futures.append(executor.submit(worker.process_with_shared_resource, shared_queue, result_dict))
                wait(futures, return_when=ALL_COMPLETED)

            self._result = dict(result_dict)

    def run_in_pool_with_chunks(self, task_range: range):
        num_of_workers = self.__max_workers
        chunks = self._get_chunks(task_range, num_of_workers)

        with ProcessPoolExecutor(max_workers=self.__max_workers) as executor:
            futures: List[Future] = list()
            worker: MultiProcessWorker
            for idx, worker in enumerate(self._workers):
                futures.append(executor.submit(worker.process_with_chunks, chunks[idx]))
            wait(futures, return_when=ALL_COMPLETED)

            for future in futures:
                name, primes = future.result()
                self._result[name] = primes

    @staticmethod
    def _get_chunks(task_range: range, num_of_chunk: int):
        tasks = tuple(task_range)
        total = len(tasks)
        chunk_size = math.ceil(total / num_of_chunk)
        chunks = [tasks[i:i + chunk_size] for i in range(0, total, chunk_size)]
        return chunks


if __name__ == '__main__':
    @time_it
    def multithread_calculate(limit: int, runner: MultiProcessRunnerType, workers: int = 3):
        task_range = range(1, limit + 1)

        task_runner = MultiProcessTaskRunner(prim_task, workers, runner)
        result: dict = task_runner.execute(task_range)

        for key in sorted(result.keys()):
            values = list(result[key])
            size = len(values)
            print(key, values[0:100 if size > 100 else size])


    up_to = 1000
    multithread_calculate(up_to, MultiProcessRunnerType.SIMPLE)
    multithread_calculate(up_to, MultiProcessRunnerType.SIMPLE, 10)
    multithread_calculate(up_to, MultiProcessRunnerType.SHARED, 3)
    multithread_calculate(up_to, MultiProcessRunnerType.PROCESS_POOL, 3)
    multithread_calculate(up_to, MultiProcessRunnerType.PROCESS_POOL, 5)
    multithread_calculate(up_to, MultiProcessRunnerType.PROCESS_POOL, 10)
    multithread_calculate(up_to, MultiProcessRunnerType.CHUNKS_IN_POOL, 10)
