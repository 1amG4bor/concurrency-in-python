from abc import ABC, abstractmethod
from typing import Callable, List, Type, Union
from queue import Queue

from multi_tasking.model.const import TASK_PREPARATION_TIME
from multi_tasking.model.worker import Worker


class TaskRunner(ABC):
    _task: Callable
    _workers: List[Worker]
    _result: dict = dict()

    def __init__(self, task: Callable, max_workers: int, worker_impl: Type[Worker] = Worker, forced_delay: int = TASK_PREPARATION_TIME):
        self._task: Callable = task
        worker_names: List[str] = [f'Worker-{idx}' for idx in range(1, max_workers + 1)]
        self._workers: List[Type[Worker]] = [worker_impl(name, self._task, forced_delay) for name in worker_names]
        self._result: dict = {worker.name: set({}) for worker in self._workers}

    @abstractmethod
    def execute(self, tasks: Union[Queue, List, range]):
        pass

    @property
    def worker(self):
        return self._workers

    def __attr_getter(self, fields: List[str]):
        return {attr: self.__getattribute__(attr) for attr in fields}

    def __str__(self):
        fields = list(vars(self).keys())
        return f'{self.__class__.__name__}({self.__attr_getter(fields)})'
