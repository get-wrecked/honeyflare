#!./venv/bin/python

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from honeyflare.vault import parse_vault_url


def main():
    args = get_args()
    details = parse_vault_url(args.vault_url)
    for key, val in details._asdict().items():
        if key == 'ca_cert':
            # Print this last since it's takes up so much space to make the others easier
            # to read
            continue
        print('%s: %s' % (key, val))

    print('CA certificate:')
    print(details.ca_cert.decode('utf-8'))


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('vault_url')
    return parser.parse_args()


if __name__ == '__main__':
    main()
