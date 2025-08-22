import base64
import os
import tempfile
import urllib.parse
from pathlib import PurePosixPath

import hvac
import requests


def get_vault_secret(vault_url):
    """
    Fetch honeycomb API token from Vault.

    The url should be on the form
    `vault://<host>:<port>/<mount-point>/<secret-path>?key=<lookup-key>&ca=<urlsafe-b64-ca-cert>`

    In addition to the `key` and `ca` query parameters you can also specify `https=false`
    to request the secret over plain http.
    """
    parsed_url = urllib.parse.urlparse(vault_url)
    parsed_parameters = urllib.parse.parse_qs(parsed_url.query)

    scheme = "https"
    https = parsed_parameters.get("https")
    if https == "false":
        scheme = "http"

    b64_ca_cert = parsed_parameters.get("ca")
    if b64_ca_cert:
        ca_cert = base64.urlsafe_b64decode(b64_ca_cert[0].encode("utf-8"))
        with tempfile.NamedTemporaryFile(delete=False) as fh:
            fh.write(ca_cert)
            os.environ["REQUESTS_CA_BUNDLE"] = fh.name

    vault_url = "%s://%s" % (scheme, parsed_url.netloc)
    vault_role = parsed_parameters.get("role", "honeyflare")
    client = hvac.Client(url=vault_url)

    client.auth.gcp.login(role=vault_role, jwt=get_auth_jwt(vault_role))

    parsed_path = PurePosixPath(parsed_url.path)

    honeycomb_key = client.secrets.kv.read_secret_version(
        path="/".join(parsed_path.parts[2:]),
        mount_point=parsed_path.parts[1],
    )
    key = parsed_parameters["key"][0]

    if b64_ca_cert:
        # Clean up to not affect other https queries
        del os.environ["REQUESTS_CA_BUNDLE"]

    return honeycomb_key["data"]["data"][key]


def get_auth_jwt(vault_role):
    url = (
        "http://metadata/computeMetadata/v1/instance/service-accounts/default/identity"
    )
    response = requests.get(
        url,
        params={
            "audience": "vault/%s" % vault_role,
        },
        headers={
            "Metadata-Flavor": "Google",
        },
        timeout=30,
    )
    response.raise_for_status()
    return response.text
