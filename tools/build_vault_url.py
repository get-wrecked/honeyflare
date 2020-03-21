#!./venv/bin/python

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from honeyflare.vault import encode_cacert


def main():
    args = get_args()
    encoded_cacert = encode_cacert(args.cacert)
    print('vault://%s/%s?key=%s%s&ca=%s' % (
        args.vault_url,
        args.path,
        args.key,
        '&role=%s' % args.role if args.role else '',
        encoded_cacert))


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('vault_url')
    parser.add_argument('path')
    parser.add_argument('-k', '--key', default='key',
        help='Under which key the secret lies. Default: %(default)s', )
    parser.add_argument('cacert', help='Path to the CA cert in PEM format')
    parser.add_argument('-r', '--role', help='A vault role to use. Default: honeyflare')
    return parser.parse_args()


if __name__ == '__main__':
    main()
