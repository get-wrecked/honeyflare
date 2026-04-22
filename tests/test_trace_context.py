from opentelemetry import trace

from honeyflare import _build_trace_context, _RayIdGenerator


def test_standalone_request_uses_ray_id_for_trace_and_span():
    """Standalone requests (no ParentRayID, or "00") have trace_id =
    span_id = RayID — the worker-request convention libhoney used. Each
    request's subrequests, which carry ParentRayID = this RayID, will
    share the same trace_id and link their parent_span_id to this
    span's span_id so Refinery reassembles a multi-span trace."""
    context, trace_id, span_id = _build_trace_context("6f2de346beec9644", "00")

    assert trace_id == 0x6f2de346beec9644
    assert span_id == 0x6f2de346beec9644
    # Empty context → span has no parent, it's a root.
    assert not trace.get_current_span(context).get_span_context().is_valid


def test_worker_subrequest_joins_parent_trace():
    """Subrequests (ParentRayID set) carry a parent context whose
    trace_id and span_id both derive from ParentRayID. That means:
      - OTel inherits trace_id = ParentRayID-derived on the new span.
      - The new span's parent_span_id = ParentRayID-derived, which
        matches the worker request's span_id (see test above) so
        Refinery links them into the same trace.
    The caller is expected to feed span_id = RayID-derived via the
    IdGenerator so subrequests keep stable span IDs too."""
    context, trace_id, span_id = _build_trace_context(
        "bbbbbbbbbbbbbbbb", "aaaaaaaaaaaaaaaa"
    )

    parent_sc = trace.get_current_span(context).get_span_context()
    assert parent_sc.trace_id == 0xaaaaaaaaaaaaaaaa
    assert parent_sc.span_id == 0xaaaaaaaaaaaaaaaa
    assert trace_id == 0xaaaaaaaaaaaaaaaa
    assert span_id == 0xbbbbbbbbbbbbbbbb


def test_missing_ray_returns_empty_context_and_none_ids():
    """Entries without a RayID fall through to OTel-generated random IDs.
    The caller sees None for trace_id/span_id and should skip set_next."""
    context, trace_id, span_id = _build_trace_context(None, None)

    assert trace_id is None
    assert span_id is None
    assert not trace.get_current_span(context).get_span_context().is_valid


def test_ray_id_generator_yields_set_ids_then_falls_back():
    gen = _RayIdGenerator()

    gen.set_next(trace_id=0x1234, span_id=0xabcd)
    assert gen.generate_trace_id() == 0x1234
    assert gen.generate_span_id() == 0xabcd

    # Second call with no set_next should return random IDs, not the
    # stale ones from the previous set_next.
    t1 = gen.generate_trace_id()
    s1 = gen.generate_span_id()
    t2 = gen.generate_trace_id()
    s2 = gen.generate_span_id()
    assert t1 != 0x1234 and t2 != 0x1234
    assert s1 != 0xabcd and s2 != 0xabcd
    # Also, random generator shouldn't collide with itself
    assert t1 != t2
    assert s1 != s2


def test_ray_id_generator_set_next_overwrites_previous():
    gen = _RayIdGenerator()
    gen.set_next(trace_id=0x1, span_id=0x2)
    gen.set_next(trace_id=0x3, span_id=0x4)
    assert gen.generate_trace_id() == 0x3
    assert gen.generate_span_id() == 0x4
