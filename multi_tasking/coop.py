import os
from queue import Queue
from time import sleep
from typing import Callable, Set

from multi_tasking.helper.prime_helper import prim_task, time_it
from multi_tasking.model import TaskRunner, Worker, Status
from multi_tasking.model.const import TASK_PREPARATION_TIME


class CoopWorker(Worker):
    def __init__(self, name: str, task: Callable):
        super().__init__(name, task)
        print(self.name, 'Worker created..', f'(PID:{os.getpid()})')

    def process(self, task_input, result: Set):
        self._status = Status.BUSY
        sleep(TASK_PREPARATION_TIME)  # Prepare for the job
        out = self._task(task_input)
        result = set({})
        if out:
            result.add(out)
        self._status = Status.IDLE


class CoopTaskRunner(TaskRunner):
    def __init__(self, task: Callable, max_workers: int):
        super().__init__(task, max_workers, CoopWorker)

    def execute(self, task_queue: Queue):
        print('Processing...')
        while not task_queue.empty():
            task = task_queue.get()
            worker = self.__select_idle_worker()
            worker.process(task, self._result[worker.name])

        return self._result

    def __select_idle_worker(self):
        while True:
            for worker in self._workers:
                if worker.is_idle():
                    return worker


if __name__ == '__main__':
    @time_it
    def cooperated_calculate(limit):
        task_queue = Queue()
        for n in range(1, limit + 1):
            task_queue.put(n)

        task_runner = CoopTaskRunner(prim_task, 2)
        result = task_runner.execute(task_queue)
        print(result)

    up_to = 100
    cooperated_calculate(up_to)
