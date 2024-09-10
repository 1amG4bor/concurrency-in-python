import cProfile
import os
import pstats
from pathlib import Path
from timeit import default_timer

from multi_tasking.helper.primes import is_prime

ROOT_DIR: Path = Path(os.path.dirname(os.path.abspath(__file__))).parents[1]


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


def cprofiler(func, *args, **kwargs):
    fn_name = func.__name__
    def wrapper(*args, **kwargs):
        profiler = cProfile.Profile()
        profiler.enable()

        result = func(*args, **kwargs)

        profiler.disable()
        stats = pstats.Stats(profiler).sort_stats('cumtime')
        stats.print_stats()
        stats.dump_stats(str(ROOT_DIR.joinpath(f'{func.__name__}_cprofile_results.prof')))
        return result

    return wrapper
