from queue import Queue
from typing import Callable

from multi_tasking.helper.prime_helper import time_it, prim_task
from multi_tasking.iii_asynchronous import AsyncTaskRunner, AsyncRunnerType
from multi_tasking.iv_multi_threading import MultiThreadTaskRunner, MultiThreadRunnerType
from multi_tasking.v_multi_processing import MultiProcessTaskRunner, MultiProcessRunnerType

NO_DELAY = 0


@time_it
def async_task_execution(task_queue: Queue, num_of_worker):
    task_runner = AsyncTaskRunner(prim_task, num_of_worker, AsyncRunnerType.TASK_GROUP, NO_DELAY)
    result = task_runner.execute(task_queue)
    return result


@time_it
def multithread_task_execution(task_queue: Queue, num_of_worker):
    task_runner = MultiThreadTaskRunner(prim_task, num_of_worker, MultiThreadRunnerType.THREAD_POOL, NO_DELAY)
    result = task_runner.execute(task_queue)
    return result


@time_it
def multiprocess_task_execution(tasks: range, num_of_worker):
    task_runner = MultiProcessTaskRunner(prim_task, num_of_worker, MultiProcessRunnerType.CHUNKS_IN_POOL, NO_DELAY)
    result = task_runner.execute(tasks)
    return result


@time_it
def move_data_to_shared_queue(task_queue, shared_queue):
    while not task_queue.empty():
        shared_queue.put(task_queue.get())


if __name__ == '__main__':
    up_to = 100_000_000
    workers = 10

    def create_number_queue(limit: int):
        print(f'Creating number queue from 1 to {limit:_}')
        num_queue = Queue()
        for n in range(1, limit + 1):
            num_queue.put(n)
        return num_queue


    def run_execution(name, task_runner: Callable, task_queue: Queue | range, num_of_workers: int = 10):
        print(f'{name} is running...')
        result: dict = task_runner(task_queue, num_of_workers)

        sum = 0
        for primes in result.values():
            sum += len(primes)
        print(f'Number of calculated prime numbers: {sum}')
        print(20 * '-')

    run_execution('ASYNC_TASK_EXECUTION', task_runner=async_task_execution,
                  task_queue=create_number_queue(up_to), num_of_workers=workers)
    run_execution('MULTITHREAD_TASK_EXECUTION', task_runner=multithread_task_execution,
                  task_queue=create_number_queue(up_to), num_of_workers=workers)
    run_execution('MULTIPROCESS_TASK_EXECUTION', task_runner=multiprocess_task_execution,
                  task_queue=range(1, up_to + 1), num_of_workers=workers)
