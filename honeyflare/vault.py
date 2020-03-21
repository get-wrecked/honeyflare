import base64
import os
import tempfile
import textwrap
import urllib.parse
import zlib
from pathlib import PurePosixPath
from collections import namedtuple

import hvac
import requests

PEM_HEADER = b'-----BEGIN CERTIFICATE-----\n'
PEM_TRAILER = b'-----END CERTIFICATE-----'

VaultCoordinates = namedtuple('VaultCoordinates', 'scheme netloc mount_point path key ca_cert role')


def get_vault_secret(vault_url):
    '''
    Fetch honeycomb API token from Vault.

    The url should be on the form
    `vault://<host>:<port>/<mount-point>/<secret-path>?key=<lookup-key>&ca=<urlsafe-b64-ca-cert>`

    In addition to the `key` and `ca` query parameters you can also specify `https=false`
    to request the secret over plain http.
    '''
    vault_coordinates = parse_vault_url(vault_url)
    if vault_coordinates.ca_cert:
        with tempfile.NamedTemporaryFile(delete=False) as fh:
            fh.write(vault_coordinates.ca_cert)
            os.environ['REQUESTS_CA_BUNDLE'] = fh.name

    vault_url = '%s://%s' % (vault_coordinates.scheme, vault_coordinates.netloc)
    client = hvac.Client(url=vault_url)

    vault_role = vault_coordinates.role
    client.auth.gcp.login(role=vault_role, jwt=get_auth_jwt(vault_role))

    honeycomb_key = client.secrets.kv.read_secret_version(
        path=vault_coordinates.path,
        mount_point=vault_coordinates.mount_point,
    )

    # Clean up to not affect other https queries
    del os.environ['REQUESTS_CA_BUNDLE']

    return honeycomb_key['data']['data'][vault_coordinates.key]


def parse_vault_url(vault_url):
    parsed_url = urllib.parse.urlparse(vault_url)
    parsed_parameters = urllib.parse.parse_qs(parsed_url.query)

    scheme = 'https'
    https = parsed_parameters.get('https')
    if https == 'false':
        scheme = 'http'

    ca_cert = None
    b64_ca_cert = parsed_parameters.get('ca')
    if b64_ca_cert:
        ca_cert = decode_cacert(b64_ca_cert[0])

    vault_url = '%s://%s' % (scheme, parsed_url.netloc)
    role = parsed_parameters.get('role', 'honeyflare')

    parsed_path = PurePosixPath(parsed_url.path)

    path = '/'.join(parsed_path.parts[2:])
    mount_point = parsed_path.parts[1]
    key = parsed_parameters['key'][0]

    return VaultCoordinates(scheme, parsed_url.netloc, mount_point, path, key, ca_cert, role)


def get_auth_jwt(vault_role):
    url = 'http://metadata/computeMetadata/v1/instance/service-accounts/default/identity'
    response = requests.get(url,
        params={
            'audience': 'vault/%s' % vault_role,
        },
        headers={
            'Metadata-Flavor': 'Google',
        },
    )
    response.raise_for_status()
    return response.text


def encode_cacert(path):
    # This function is only called by ./tools/build_vault_url.py, keeping it here to keep
    # it together with the decoder.
    # PEM certificates are just DER certificates encoded with base64 and some wrapping.
    # To try to keep url lengths somewhat manageable, compress the DER certificate and
    # re-encode it with urlsafe base64.
    with open(path, 'rb') as fh:
        contents = fh.read().rstrip(b'\n')
        assert contents.startswith(PEM_HEADER)
        # Strip possible trailing newlines
        contents = contents.rstrip(b'\n')
        assert contents.endswith(PEM_TRAILER)
        der_cert_base64 = contents[len(PEM_HEADER):-len(PEM_TRAILER)]
        der_cert = base64.b64decode(der_cert_base64)
        compressed_der_cert = zlib.compress(der_cert)
        return base64.urlsafe_b64encode(compressed_der_cert).decode('utf-8').rstrip('=')


def decode_cacert(b64_ca_cert):
    b64_ca_cert += '=' * (-len(b64_ca_cert) % 4) # restore padding
    compressed_der_cert = base64.urlsafe_b64decode(b64_ca_cert.encode('utf-8'))
    der_cert = zlib.decompress(compressed_der_cert)
    pem_body = textwrap.wrap(base64.b64encode(der_cert).decode('utf-8'), width=64)
    return PEM_HEADER + '\n'.join(pem_body).encode('utf-8') + b'\n' + PEM_TRAILER
