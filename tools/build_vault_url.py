#!./venv/bin/python

import argparse
import base64

def main():
    args = get_args()
    encoded_cacert = encode_cacert(args.cacert)
    print('vault://%s/%s?key=%s%s&ca=%s' % (
        args.vault_url,
        args.path,
        args.key,
        '&role=%s' % args.role if args.role else '',
        encoded_cacert))


def encode_cacert(path):
    with open(path, 'rb') as fh:
        contents = fh.read().rstrip(b'\n')
        return base64.urlsafe_b64encode(contents).decode('utf-8')


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('vault_url')
    parser.add_argument('path')
    parser.add_argument('-k', '--key', default='key',
        help='Under which key the secret lies. Default: %(default)s', )
    parser.add_argument('cacert', help='Path to the CA cert')
    parser.add_argument('-r', '--role', help='A vault role to use. Default: honeyflare')
    return parser.parse_args()


if __name__ == '__main__':
    main()
