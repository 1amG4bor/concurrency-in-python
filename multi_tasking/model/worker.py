import os
from enum import Enum
from typing import Callable, List
from queue import Queue
from time import sleep
from multi_tasking.model.const import TASK_PREPARATION_TIME


class Status(Enum):
    IDLE = 'Ready to work'
    BUSY = 'Work in progress'
    DEAD = 'Fatal error'


class Worker:
    def __init__(self, name: str, task: Callable):
        self.name: str = name
        self._task: Callable = task
        self._status: Status = Status.IDLE

    def is_idle(self):
        return self._status is Status.IDLE

    def process(self, task_queue: Queue, result: List[any]):
        print(self.name, 'start working..', f'(PID:{os.getpid()})')
        self._status = Status.BUSY
        while not task_queue.empty():
            input = task_queue.get()
            sleep(TASK_PREPARATION_TIME)  # Prepare for the job
            out = self._task(input)
            if out:
                result.add(out)
        self._status = Status.IDLE
