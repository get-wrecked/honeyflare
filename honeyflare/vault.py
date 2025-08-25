import base64
import os
import tempfile
import urllib.parse
from pathlib import PurePosixPath

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


def get_vault_secret(vault_url):
    """
    Fetch honeycomb API token from Vault.

    The url should be on the form
    `vault://<host>:<port>/<mount-point>/<secret-path>?key=<lookup-key>&ca=<urlsafe-b64-ca-cert>`

    In addition to the `key` and `ca` query parameters you can also specify `https=false`
    to request the secret over plain http.
    """
    adapter = HTTPAdapter(
        max_retries=Retry(
            total=7,
            backoff_factor=1,
            backoff_jitter=10,
            status_forcelist=[412, 429, 500, 502, 503],
            allowed_methods=(
                "GET",
                "HEAD",
                # POST is safe here because we are only using it for login
                "POST",
            ),
            raise_on_status=False,
        )
    )
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session = requests.Session()
    parsed_url = urllib.parse.urlparse(vault_url)
    parsed_parameters = urllib.parse.parse_qs(parsed_url.query)

    scheme = "https"
    https = parsed_parameters.get("https")
    if https == "false":
        scheme = "http"

    b64_ca_cert = parsed_parameters.get("ca")
    request_kwargs = {}
    if b64_ca_cert:
        ca_cert = base64.urlsafe_b64decode(b64_ca_cert[0].encode("utf-8"))
        with tempfile.NamedTemporaryFile(delete=False) as fh:
            fh.write(ca_cert)
        request_kwargs["verify"] = fh.name

    vault_url = "%s://%s" % (scheme, parsed_url.netloc)
    vault_role = parsed_parameters.get("role", "honeyflare")

    login_response = session.post(
        f"{vault_url}/v1/auth/gcp/login",
        **request_kwargs,
        json={
            "role": vault_role,
            "jwt": get_auth_jwt(vault_role),
        },
    )
    login_response.raise_for_status()

    session.headers["x-vault-token"] = login_response.json()["auth"]["client_token"]

    parsed_path = PurePosixPath(parsed_url.path)

    honeycomb_key = session.get(
        f"{vault_url}/v1/{parsed_path.parts[1]}/data/{'/'.join(parsed_path.parts[2:])}"
    )
    honeycomb_key.raise_for_status()
    key = parsed_parameters["key"][0]

    return honeycomb_key.json()["data"]["data"][key]


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
