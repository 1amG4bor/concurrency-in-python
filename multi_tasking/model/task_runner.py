from abc import ABC
from typing import Callable, List, Type

from multi_tasking.model.worker import Worker


class TaskRunner(ABC):
    _task: Callable
    _workers: List[Worker]
    _result = dict()

    def __init__(self, task: Callable, max_workers: int, worker_impl: Type[Worker] = Worker):
        self._task = task
        worker_names = [f'Worker-{idx}' for idx in range(1, max_workers + 1)]
        self._workers = [worker_impl(name, self._task) for name in worker_names]
        self._result = {worker.name: set({}) for worker in self._workers}

    def __attr_getter(self, fields: List[str]):
        return {attr: self.__getattribute__(attr) for attr in fields}

    def __str__(self):
        fields = list(vars(self).keys())
        return f'{self.__class__.__name__}({self.__attr_getter(fields)})'
