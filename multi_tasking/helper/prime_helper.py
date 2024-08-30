import os
from abc import ABC
from enum import Enum
from time import sleep
from timeit import default_timer
from typing import Callable, List, Type

from multi_tasking.helper.primes import is_prime


def prim_task(num: int):
    if num and is_prime(num):
        return num


def time_it(func, *args, **kwargs):
    def wrapper(*args, **kwargs):
        start = default_timer()
        result = func(*args, **kwargs)
        end = default_timer()
        print(f'{func.__name__} took {end - start:.4f} seconds to complete.')
        return result

    return wrapper
