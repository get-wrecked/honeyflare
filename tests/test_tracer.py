from unittest import mock

from honeyflare import create_otel_tracer


def test_headers_wired_through():
    """honeycomb_key and honeycomb_dataset become the two headers
    Honeycomb's OTLP endpoint reads for routing — same contract libhoney
    had via writekey and dataset."""
    with mock.patch("honeyflare.OTLPSpanExporter") as mock_exporter:
        _, provider = create_otel_tracer(
            service_name="cloudflare",
            honeycomb_api="https://api.honeycomb.io",
            honeycomb_key="secret-key",
            honeycomb_dataset="cloudflare-prod",
        )
        provider.shutdown()

    mock_exporter.assert_called_once_with(
        endpoint="https://api.honeycomb.io/v1/traces",
        headers={
            "x-honeycomb-team": "secret-key",
            "x-honeycomb-dataset": "cloudflare-prod",
        },
    )


def test_endpoint_trailing_slash_is_stripped():
    with mock.patch("honeyflare.OTLPSpanExporter") as mock_exporter:
        _, provider = create_otel_tracer(
            service_name="cloudflare",
            honeycomb_api="https://api.honeycomb.io/",
            honeycomb_key="secret-key",
            honeycomb_dataset="cloudflare-prod",
        )
        provider.shutdown()

    mock_exporter.assert_called_once_with(
        endpoint="https://api.honeycomb.io/v1/traces",
        headers={
            "x-honeycomb-team": "secret-key",
            "x-honeycomb-dataset": "cloudflare-prod",
        },
    )
