import math
import os
from concurrent.futures import ProcessPoolExecutor
from enum import Enum
# import threading
from multiprocessing import Manager, Process, current_process, Queue
from time import sleep
from typing import Callable, Dict, Generator

from multi_tasking.helper.prime_helper import prim_task, time_it
from multi_tasking.model import TaskRunner, Worker
from multi_tasking.model.const import TASK_PREPARATION_TIME


class MultiProcessRunnerType(Enum):
    SIMPLE = "manually managed Processes"
    SHARED = "manually managed Processes with shared queue"
    PROCESS_POOL = "ProcessPoolExecutor"


class MultiProcessWorker(Worker):
    def __init__(self, name: str, task: Callable):
        super().__init__(name, task)

    def process(self, task_list, manager_dict):
        print(self.name, 'start working..', f'(PID:{os.getpid()})')
        result = set({})
        while task_list:
            sleep(TASK_PREPARATION_TIME)  # Prepare for the job
            item = task_list.pop(0)
            if not item:
                break
            # print(f'{name} checking: {item}')
            output = self._task(item)
            if output:
                result.add(output)
        manager_dict[self.name] = result

    def process_with_shared_resource(self, shared_queue: Queue, result_dict: Dict):
        name = current_process().name
        print(f'{name}: start working.. (PID:{os.getpid()})')
        result = set({})
        while not shared_queue.empty():
            # print(f'{name}: {shared_queue.qsize() } left.')
            sleep(TASK_PREPARATION_TIME)  # Prepare for the job
            try:
                item = shared_queue.get(block=False)
            except Exception:
                continue

            out = self._task(item)
            if out:
                result.add(out)

        result_dict[name] = result


class MultiProcessTaskRunner(TaskRunner):
    def __init__(self, task: Callable, max_workers: int, runner: MultiProcessRunnerType):
        super().__init__(task, max_workers, MultiProcessWorker)
        self._task = task
        self.__max_workers = max_workers
        self.__runner_type: MultiProcessRunnerType = runner

    def execute(self, task_range: Generator):
        """The entrypoint and separator of different task execution implementations."""
        print(f"\nProcessing with '{self.__runner_type.value}'... ({self.__max_workers} processes)")
        match self.__runner_type:
            case MultiProcessRunnerType.SIMPLE:
                self.run_simple(task_range)
            case MultiProcessRunnerType.SHARED:
                self.run_simple_shared(task_range)
            case MultiProcessRunnerType.PROCESS_POOL:
                self.run_in_pool(task_range)

        return self._result

    def run_simple(self, task_range: Generator):
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

    def run_simple_shared(self, task_range: Generator):
        """Execute the task with a group of processes with shared queue"""

        with Manager() as manager:
            shared_queue = manager.Queue()
            for i in task_range:
                shared_queue.put(i)
            result_dict = manager.dict()

            processes = list()
            for worker in self._workers:
                process = Process(name=worker.name,
                                  target=worker.process_with_shared_resource,
                                  args=(shared_queue, result_dict))
                process.start()
                processes.append(process)

            for process in processes:
                process.join()
            self._result = dict(result_dict)

    def run_in_pool(self, task_range: Generator):
        """Execute the task with a pool of managed threads."""
        with Manager() as manager:
            shared_queue = manager.Queue()
            for i in task_range:
                shared_queue.put(i)
            result_dict = manager.dict()

            with ProcessPoolExecutor(max_workers=self.__max_workers) as executor:
                for worker in self._workers:
                    executor.submit(worker.process_with_shared_resource, shared_queue, result_dict)

            self._result = dict(result_dict)

    @staticmethod
    def _get_chunks(task_range: Generator, num_of_chunk: int):
        full_list = list(task_range)
        total = len(full_list)
        chunk_size = math.ceil(total / num_of_chunk)
        chunks = []
        for i in range(num_of_chunk):
            start = i * chunk_size
            chunks.append(full_list[start:start + chunk_size])

        return chunks

    def _pool_processing(self, shared_queue: Queue, result_dict: Dict):
        name = current_process().name
        print(f'{name}: start working.. (PID:{os.getpid()})')
        result = set({})
        while not shared_queue.empty():
            sleep(TASK_PREPARATION_TIME)  # Prepare for the job
            item = shared_queue.get()
            out = self._task(item)
            if out:
                result.add(out)

        result_dict[name] = result


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


    up_to = 100
    print("GENERATED DELAY:", up_to * 0.01, 'sec')
    multithread_calculate(up_to, MultiProcessRunnerType.SIMPLE)
    multithread_calculate(up_to, MultiProcessRunnerType.SIMPLE, 10)
    multithread_calculate(up_to, MultiProcessRunnerType.SHARED, 2)
    multithread_calculate(up_to, MultiProcessRunnerType.PROCESS_POOL, 3)
    multithread_calculate(up_to, MultiProcessRunnerType.PROCESS_POOL, 5)
