import os
from concurrent.futures import ProcessPoolExecutor, Future, ALL_COMPLETED, wait
from typing import List

from multi_tasking.helper.prime_helper import prim_task, time_it


def process_with_chunks(range_interval: tuple[int, int]):
    start, end = range_interval
    print(f'Start working with the process of numbers from {start} to {end} [{os.getpid()}]..')
    result = set({})
    for item in range(start, end):
        output = prim_task(item)
        if output:
            result.add(output)
    return result


@time_it
def calc_primes(max_num: int, workers: int = 10):
    print(f'Start calculating the primes up to {max_num:_} using {workers} worker processes...')

    chunk_size = max_num // workers
    ranges = [(i + 1, i + chunk_size) for i in range(0, max_num, chunk_size)]

    result: set = set({})
    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures: List[Future] = list()
        for idx in range(0, workers):
            futures.append(executor.submit(process_with_chunks, ranges[idx]))
        wait(futures, return_when=ALL_COMPLETED)

        for future in futures:
            prime_numbers = future.result()
            result = result.union(prime_numbers)

    print(f'Primes up to {max_num:_} are calculated. {len(result)} primes can be found.')
    return result


if __name__ == '__main__':
    up_to = 10_000_000
    primes = list(calc_primes(up_to, 10))
    print(primes[:5], '...', primes[-5:])
