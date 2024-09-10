import asyncio
import os
from enum import Enum
from queue import Queue
from typing import Callable, List

from multi_tasking.helper.prime_helper import prim_task, time_it
from multi_tasking.model import TaskRunner, Worker, Status
from multi_tasking.model.const import TASK_PREPARATION_TIME


class AsyncRunnerType(Enum):
    SIMPLE = "manually managed Tasks"
    TASK_GROUP = "TaskGroup managed Task"


class AsyncWorker(Worker):
    def __init__(self, name: str, task: Callable, delay: int = TASK_PREPARATION_TIME):
        super().__init__(name, task, delay)

    async def process(self, task_queue: Queue, result: List[any]):
        print(self.name, 'start working..', f'(PID:{os.getpid()})')
        self._status = Status.BUSY
        while not task_queue.empty():
            item = task_queue.get()
            await asyncio.sleep(self._forced_delay)  # Forced delay to indicate preparation for the job
            out = self._task(item)
            if out:
                result.add(out)
        self._status = Status.IDLE


class AsyncTaskRunner(TaskRunner):
    def __init__(self, task: Callable, max_workers: int, runner: AsyncRunnerType = AsyncRunnerType.SIMPLE,
                 forced_delay: int = None):
        super().__init__(task, max_workers, AsyncWorker, forced_delay)
        self.__max_workers = max_workers
        self.__runner_type: AsyncRunnerType = runner

    def execute(self, work_queue: Queue, show_progress: bool = True):
        print(f"\nProcessing with '{self.__runner_type.value}'... ({self.__max_workers} worker)")
        match self.__runner_type:
            case AsyncRunnerType.SIMPLE:
                asyncio.run(self._simple_manual_execution(work_queue, show_progress))
            case AsyncRunnerType.TASK_GROUP:
                asyncio.run(self._taskgroup_execution(work_queue, show_progress))

        return self._result

    async def _simple_manual_execution(self, work_queue: Queue, show_progress):

        tasks = list()
        for worker in self._workers:
            print(f'Creating task for {worker.name}')
            tasks.append(
                asyncio.create_task(
                    worker.process(work_queue, self._result[worker.name])))

        if show_progress:
            while not work_queue.empty():
                print(f'Remaining tasks: {work_queue.qsize()}')
                await asyncio.sleep(1)
        else:
            await asyncio.wait(tasks)

        return self._result

    async def _taskgroup_execution(self, work_queue: Queue, show_progress):
        print('Processing...')
        tasks = list()
        async with asyncio.TaskGroup() as group:
            for worker in self._workers:
                print(f'Creating task for {worker.name}')
                task = group.create_task(worker.process(work_queue, self._result[worker.name]))
                tasks.append(task)

        if show_progress:
            while not work_queue.empty():
                print(f'Remaining tasks: {work_queue.qsize()}')
                await asyncio.sleep(1)

        for task in tasks:
            await task

        return self._result


if __name__ == '__main__':
    @time_it
    def asynchron_calculate(limit: int, workers: int = 2, runner_type: AsyncRunnerType = AsyncRunnerType.SIMPLE):
        task_queue = Queue()
        for n in range(1, limit + 1):
            task_queue.put(n)

        task_runner = AsyncTaskRunner(prim_task, workers, runner_type)
        result = task_runner.execute(task_queue)
        print(result)


    up_to = 1000
    asynchron_calculate(up_to)
    asynchron_calculate(up_to, 3)
    asynchron_calculate(up_to, 3, AsyncRunnerType.TASK_GROUP)
