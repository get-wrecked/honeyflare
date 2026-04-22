from opentelemetry import trace

from honeyflare import _build_trace_context


def test_standalone_request_returns_empty_context():
    """Standalone requests (no ParentRayID, or '00') fall through to OTel's
    random trace/span_id generation — no fake parent."""
    ctx = _build_trace_context("6f2de346beec9644", "00")
    span = trace.get_current_span(ctx)
    assert not span.get_span_context().is_valid


def test_worker_subrequest_inherits_parent_trace():
    """When a worker-initiated subrequest has ParentRayID, the resulting
    child trace_id equals ParentRayID so parent worker + child upstream
    requests land in the same Honeycomb trace."""
    ctx = _build_trace_context("bbbbbbbbbbbbbbbb", "aaaaaaaaaaaaaaaa")
    span = trace.get_current_span(ctx)
    sc = span.get_span_context()

    assert sc.trace_id == 0xaaaaaaaaaaaaaaaa
    assert sc.span_id == 0xaaaaaaaaaaaaaaaa


def test_missing_ray_returns_empty_context():
    """Entries without a RayID fall through to OTel-generated random IDs."""
    ctx = _build_trace_context(None, None)
    span = trace.get_current_span(ctx)
    assert not span.get_span_context().is_valid
