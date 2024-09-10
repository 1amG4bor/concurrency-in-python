import math
from timeit import default_timer
from typing import List

from multi_tasking.helper.prime_list import primes


def is_prime(n: int):
    if n in [2, 3]:
        return True
    elif n <= 1 or n % 2 == 0:
        return False
    else:
        factors = range(3, int(math.sqrt(n)) + 1, 2)
        # print(f'Factors are:', list(factors))
        for i in factors:
            # print(f'{n} % {i} = {n%i}')
            if n % i == 0:
                return False
        return True


def __spec_twins(num_list: List[int], diff):
    """Returns a collection of number pairs with the predefined difference between them."""
    twins = []
    for idx, num in enumerate(num_list):
        if not is_prime(num):
            raise ValueError("The provided list of integers contains non-prime number(s).")
        if (idx > 0) and (num == num_list[idx-1] + diff):
            twins.append([num_list[idx-1], num])
    return twins


def twin_primes(p_list: List[int]):
    """Sexy primes are prime numbers pairs that differ by two."""
    return __spec_twins(p_list, 2)


def cousin_primes(p_list: List[int]):
    """Cousin primes are prime numbers pairs that differ by four."""
    return __spec_twins(p_list, 4)


def sexy_primes(p_list: List[int]):
    """Sexy primes are prime numbers pairs that differ by six."""
    twins = []
    for idx, num in enumerate(p_list):
        if not is_prime(num):
            raise ValueError("The provided list of integers contains non-prime number(s).")
        if (idx > 0) and (num == p_list[idx - 1] + 6):
            twins.append([p_list[idx - 1], num])
        if (idx > 1) and (num == p_list[idx - 2] + 6):
            twins.append([p_list[idx - 2], num])
    return twins


def prime_triplets(num_list: List[int]):
    """Prime triplet is a set of three prime numbers in which the smallest and largest of the three differ by 6."""
    triplets = []
    for idx, num in enumerate(num_list):
        if not is_prime(num):
            raise ValueError("The provided list of integers contains non-prime number(s).")
        pre_1 = num_list[idx - 1]
        pre_2 = num_list[idx - 2]
        if (idx > 1) and (pre_2 == num - 6) and (pre_1 == num - 4 or pre_1 == num - 2):
            triplets.append([pre_2, pre_1, num])
    return triplets


def is_permutable_prime(num: int):
    """ Permutable prime/anagrammatic prime.
    Prime number, which have its digits' positions switched through any permutation and still be a prime number.
    """
    reversed_num = int(str(num)[::-1])
    return is_prime(num) and is_prime(reversed_num)


def calc_primes(n: int, show_progress: bool = True):
    progress_step = n // 10
    print('Start calculating...')
    start = default_timer()
    my_primes = []
    for i in range(1, n + 1):
        is_p = is_prime(i)
        if is_p:
            my_primes.append(i)
        if show_progress and (i % progress_step == 0 or i == n):
            print(f'Progress: {i / n * 100:.0f}%, {len(my_primes):_} primes found so far..')

    end = default_timer()
    print(f'Primes up to {n:_} are calculated in: {end-start:4f} sec(s)')
    return my_primes


def test_prime_calc(prime_list: List[int], n):
    expected_primes = [i for i in primes if i < n]

    print(f"expected: {len(expected_primes)}, actual: {len(prime_list)}")
    assert prime_list == expected_primes
    print(f'Calculation checked!')


if __name__ == "__main__":
    up_to = 100_000_000
    my_primes = calc_primes(up_to)
    # print(f'My first 100 primes: {my_primes[:100]}')
    # print(f'My last 10 primes up to {up_to:_}: {my_primes[-10:]}')

    # Uncomment the next line to check the prime numbers up to 100_000.
    # print(test_prime_calc(my_primes, up_to))

    # Special Primes
    # print("Checking special primes...")
    # my_twins = twin_primes(my_primes)
    # my_cousins = cousin_primes(my_primes)
    # my_sexy_primes = sexy_primes(my_primes)
    # my_perm_primes = [[n, int(str(n)[::-1])] for n in filter(is_permutable_prime, my_primes)]
    # my_prime_triplets = prime_triplets(my_primes)
    #
    # print(f'My first twin primes{my_twins[:100]}')
    # print(f'My first cousin primes: {my_cousins[:100]}')
    # print(f'My first sexy primes: {my_sexy_primes[:100]}')
    # print(f'My first permutable primes: {my_perm_primes[:100]}')
    # print(f'My first prime triplets: {my_prime_triplets[:100]}')
