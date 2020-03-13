#!./venv/bin/python

'''
Helper to compute the expected range for a correctly implemented sampler
(Bernoulli trials) with a given test success rate.
'''

import argparse
import math
from decimal import Decimal


def main():
    args = get_args()
    success_probability = Decimal(1.0 / args.sample_rate)
    expected_value = int(args.trials * success_probability)
    cumulative = pmf(args.trials, expected_value, success_probability)
    if args.sample_rate == 1:
        deviation = 0
    else:
        deviation = 1
        while cumulative < args.success_rate:
            # Gradually try a larger deviation until the cumulative is above the target percentage
            cumulative += pmf(args.trials, expected_value - deviation, success_probability)
            cumulative += pmf(args.trials, expected_value + deviation, success_probability)
            deviation += 1
            if deviation >= args.trials:
                break

    print('Expected value: %d' % expected_value)
    print('The test should assert %d-%d successes to succeed in %.4f%% of runs' % (
        max(expected_value - deviation, 0), min(expected_value + deviation, args.trials), cumulative))


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('trials', type=int)
    parser.add_argument('sample_rate', type=int)
    parser.add_argument('-r', '--success-rate', default=.9999, type=float)
    return parser.parse_args()


def pmf(n, k, p):
    return binomial(n, k)*(p**k)*(1 - p)**(n - k)


def binomial(n, k):
    '''
    Compute the binomial coefficient (n k).

    :param n: the size of the pile of elements
    :param k: the number of elements to take from the pile
    :return: the number of ways to choose k elements out of a pile of n
    '''
    # Based on https://stackoverflow.com/a/46778364

    if k < 0 or k > n:
        return 0

    if k == 0 or k == n:
        return 1

    total_ways = 1
    for i in range(min(k, n - k)):
        total_ways = total_ways * (n - i) // (i + 1)

    return total_ways


if __name__ == '__main__':
    main()
