from queue import Queue
from typing import Callable

from multi_tasking.helper.prime_helper import prim_task, time_it
from multi_tasking.model import TaskRunner


class SyncTaskRunner(TaskRunner):
    def __init__(self, task: Callable, max_workers: int, delay: int = None):
        super().__init__(task, max_workers, forced_delay=delay)

    def execute(self, task_queue: Queue):
        print('Processing...')
        for worker in self._workers:
            worker.process(task_queue, self._result[worker.name])

        return self._result


if __name__ == '__main__':
    @time_it
    def synchron_calculate(limit):
        task_queue = Queue()
        for n in range(1, limit + 1):
            task_queue.put(n)

        task_runner = SyncTaskRunner(task=prim_task, max_workers=2)
        result = task_runner.execute(task_queue)
        print(result)

    up_to = 1000
    synchron_calculate(up_to)
