import os
from enum import Enum
from typing import Callable, List
from queue import Queue
from time import sleep

from line_profiler import profile

from multi_tasking.model.const import TASK_PREPARATION_TIME


class Status(Enum):
    IDLE = 'Ready to work'
    BUSY = 'Work in progress'
    DEAD = 'Fatal error'


class Worker:
    def __init__(self, name: str, task: Callable, delay: int = TASK_PREPARATION_TIME):
        self.name: str = name
        self._task: Callable = task
        self._status: Status = Status.IDLE
        self._forced_delay: int = delay if isinstance(delay, int) and delay >= 0 else TASK_PREPARATION_TIME

    def is_idle(self):
        return self._status is Status.IDLE

    def process(self, task_queue: Queue, result: List[any]):
        print(self.name, 'start working..', f'(PID:{os.getpid()})')
        self._status = Status.BUSY
        while not task_queue.empty():
            input = task_queue.get()
            sleep(self._forced_delay)  # Forced delay to indicate preparation for the job
            out = self._task(input)
            if out:
                result.add(out)
        self._status = Status.IDLE
