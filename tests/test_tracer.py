from unittest import mock

from honeyflare import create_otel_tracer


def test_exporter_points_at_otlp_traces_endpoint():
    """honeyflare sends OTLP to `${honeycomb_api}/v1/traces` — no headers,
    no ingest key. The deploy target is expected to be a Refinery with
    SendKeyMode: missingonly that injects the key on egress."""
    with mock.patch("honeyflare.OTLPSpanExporter") as mock_exporter:
        _, provider = create_otel_tracer(
            service_name="cloudflare",
            honeycomb_api="http://refinery.local",
        )
        provider.shutdown()

    mock_exporter.assert_called_once_with(
        endpoint="http://refinery.local/v1/traces",
    )


def test_endpoint_trailing_slash_is_stripped():
    with mock.patch("honeyflare.OTLPSpanExporter") as mock_exporter:
        _, provider = create_otel_tracer(
            service_name="cloudflare",
            honeycomb_api="http://refinery.local/",
        )
        provider.shutdown()

    mock_exporter.assert_called_once_with(
        endpoint="http://refinery.local/v1/traces",
    )
