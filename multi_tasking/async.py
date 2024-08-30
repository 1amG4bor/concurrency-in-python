import asyncio
import os
from queue import Queue
from typing import Callable

from multi_tasking.helper.prime_helper import prim_task, time_it
from multi_tasking.model import TaskRunner, Worker, Status
from multi_tasking.model.const import TASK_PREPARATION_TIME


# Async/await - coroutine
# Event-loop (get_event_loop() => run())
# Others:
#   .gather(t1, t2) // task, coroutine
#   .wait(tasklist) // list, dict or set
#   .TaskGroup => `async with asyncio.TaskGroup() as group: ..task = group.create_task(fn)`
# Other async related libs: `aiofiles`, `aiohttp`


class AsyncWorker(Worker):
    def __init__(self, name: str, task: Callable):
        super().__init__(name, task)

    async def process(self, task_queue, result):
        print(self.name, 'start working..', f'(PID:{os.getpid()})')
        self._status = Status.BUSY
        while not task_queue.empty():
            item = task_queue.get()
            await asyncio.sleep(TASK_PREPARATION_TIME)  # Prepare for the job
            out = self._task(item)
            if out:
                result.add(out)
        self._status = Status.IDLE


class AsyncTaskRunner(TaskRunner):
    def __init__(self, task: Callable, max_workers: int):
        super().__init__(task, max_workers, AsyncWorker)

    def execute(self, work_queue: Queue, show_progress: bool = True):
        asyncio.run(self._async_execution(work_queue, show_progress))
        return self._result

    async def _async_execution(self, task_queue: Queue, show_progress):
        print('Processing...')
        tasks = list()
        for worker in self._workers:
            tasks.append(
                asyncio.create_task(
                    worker.process(task_queue, self._result[worker.name])))

        if show_progress:
            while not task_queue.empty():
                print(f'Remaining tasks: {task_queue.qsize()}')
                await asyncio.sleep(0.01)

        for task in tasks:
            await task

        return self._result


if __name__ == '__main__':
    @time_it
    def asynchron_calculate(limit):
        task_queue = Queue()
        for n in range(1, limit + 1):
            task_queue.put(n)

        task_runner = AsyncTaskRunner(prim_task, 3)
        result = task_runner.execute(task_queue)
        print(result)

    up_to = 100
    asynchron_calculate(up_to)
