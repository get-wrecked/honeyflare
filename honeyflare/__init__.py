import gzip
import os
import time

import orjson
from google.api_core.exceptions import PreconditionFailed
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.id_generator import IdGenerator, RandomIdGenerator
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from urllib3.exceptions import HTTPError

from . import enrichment
from .exceptions import RetriableError
from .locks import GCSLock
from .sampler import Sampler
from .urlshape import compile_pattern
from .version import __version__


def process_bucket_object(
    bucket,
    object_name,
    honeycomb_api="https://api.honeycomb.io",
    patterns=None,
    query_param_filter=None,
    sampling_rate_by_status=None,
    lock_bucket=None,
):
    """
    :param bucket: A `google.cloud.storage.bucket.Bucket` logs should be
        downloaded from.
    :param object_name: The name of the object in the bucket to download.
    :param honeycomb_api: The base URL OTLP traces are sent to. Typically a
        Refinery configured with `SendKeyMode: missingonly` so the ingest
        key is injected on egress; Honeycomb E&S routes by `service.name`.
    :param patterns: A list of path patterns to match against.
    :param query_param_filter: A set of query parameters to allow. If None, all
        will be allowed. If empty, none.
    :param sampling_rate_by_status: A dictionary mapping a status code to a
        sampling rate. Ie {200: 10, 400: 1}. A specific match will be checked
        first (ie {404: 10}), then the general class of code (ie 400 for a 404).
    :param lock_bucket: If you want to use a dedicated bucket for holding locks
        and completion status, pass it here. Otherwise the bucket that holds the
        logs will be used (requires write access to that bucket).
    """
    if sampling_rate_by_status is None:
        sampling_rate_by_status = {}

    if lock_bucket is None:
        lock_bucket = bucket

    compiled_patterns = [compile_pattern(p) for p in patterns or []]
    id_generator = _RayIdGenerator()
    tracer, provider = create_otel_tracer(
        service_name="cloudflare",
        honeycomb_api=honeycomb_api,
        id_generator=id_generator,
    )

    lock = GCSLock(lock_bucket, "locks/%s" % object_name)
    total_events = 0
    try:
        with lock:
            if is_already_processed(lock_bucket, object_name):
                # We might have been retried due to a failure but another
                # function succeeded in the meantime
                return

            local_path = download_file(bucket, object_name)

            sampler = Sampler()
            source = get_raw_file_entries(local_path)
            for sample_rate, entry in sampler.sample_lines(source, sampling_rate_by_status):
                enrichment.enrich_entry(entry, compiled_patterns, query_param_filter)

                start_time_ns = int(entry["EdgeEndTimestamp"])
                context, trace_id, span_id = _build_trace_context(
                    entry.get("RayID"), entry.get("ParentRayID")
                )
                id_generator.set_next(trace_id=trace_id, span_id=span_id)

                span = tracer.start_span(
                    "HTTP %s" % entry.get("ClientRequestMethod", "N/A"),
                    context=context,
                    start_time=start_time_ns,
                )
                try:
                    span.set_attribute("SampleRate", sample_rate)
                    span.set_attribute(
                        "MetaProcessor", "honeyflare/%s" % __version__
                    )
                    span.set_attributes(
                        {
                            k: _coerce_attribute_value(v)
                            for k, v in entry.items()
                            if v is not None
                        }
                    )
                finally:
                    span.end(end_time=start_time_ns)
                total_events += 1

            os.remove(local_path)
            mark_as_processed(lock_bucket, object_name)
    finally:
        provider.shutdown()
    return total_events


def create_otel_tracer(service_name, honeycomb_api, id_generator=None):
    """
    Build an OTel tracer + provider pointed at a Honeycomb (or proxy)
    endpoint over OTLP HTTP. service.name is set as a resource attribute
    and is what Honeycomb E&S routes events to within the environment.

    No ingest key is sent from honeyflare — deployments are expected to
    route through a Refinery configured with `SendKeyMode: missingonly`,
    which injects the key on egress to Honeycomb.

    service.version is pinned to honeyflare's own package version, which
    tools/release.py rewrites at build time to embed the git treeish (e.g.
    "0.3.0-a1b2c3d4...") so every deployed build is distinguishable in
    Honeycomb.

    id_generator defaults to OTel's random generator. Pass a custom one
    (e.g. `_RayIdGenerator`) when span/trace IDs need to be derived from
    upstream identifiers so Refinery reassembles multi-span traces.

    Returns (tracer, provider). Callers should call provider.shutdown() at
    the end of the scope to flush buffered spans.
    """
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": __version__,
        }
    )
    provider = TracerProvider(resource=resource, id_generator=id_generator)
    exporter = OTLPSpanExporter(
        endpoint="%s/v1/traces" % honeycomb_api.rstrip("/"),
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    return provider.get_tracer("honeyflare"), provider


def _build_trace_context(ray_id, parent_ray_id):
    """
    Build an OTel context + trace/span IDs for a cloudflare log line so
    a worker request and its subrequests reassemble into a single
    multi-span trace at Refinery — same shape libhoney produced via
    trace.trace_id / trace.span_id / trace.parent_id event fields.

    Returns (context, trace_id, span_id):
      - `context` is passed to tracer.start_span; it carries the parent
        SpanContext for subrequests, or an empty Context for standalone
        requests.
      - `trace_id` and `span_id` are the 64-bit-in-128-bit integers the
        caller must inject via a custom IdGenerator before start_span, so
        OTel uses them instead of picking random ones.

    Cloudflare RayIDs are 16 hex chars (64 bits). OTel trace IDs are 128
    bits; the ray fits in the low half (high half zero) — matches the
    libhoney `uuid_from_ray_id` padding.

    Encoding matches libhoney:
      - Worker request (ParentRayID "00" or absent): trace_id = span_id =
        ray_id, no parent.
      - Subrequest:                                    trace_id =
        parent_span_id = ParentRayID, span_id = ray_id. A non-recording
        parent span carries the parent-derived IDs; the resulting span is
        a child that inherits trace_id and has parent_span_id set so
        Refinery links it to the worker's span.

    When ray_id is absent, returns (Context(), None, None) so the caller
    uses OTel's default random IDs.
    """
    if not ray_id:
        return trace.Context(), None, None

    span_id = _ray_to_int(ray_id)

    if parent_ray_id and parent_ray_id != "00":
        parent_span_id = _ray_to_int(parent_ray_id)
        trace_id = _ray_to_int(parent_ray_id)
        parent_ctx = SpanContext(
            trace_id=trace_id,
            span_id=parent_span_id,
            is_remote=True,
            trace_flags=TraceFlags(TraceFlags.SAMPLED),
        )
        context = trace.set_span_in_context(NonRecordingSpan(parent_ctx))
    else:
        trace_id = _ray_to_int(ray_id)
        context = trace.Context()

    return context, trace_id, span_id


def _ray_to_int(ray_id):
    return int(ray_id, 16)


class _RayIdGenerator(IdGenerator):
    """IdGenerator that yields pre-set trace/span IDs when available, falling
    back to random otherwise. The caller pairs each `tracer.start_span` call
    with a `set_next` to encode Cloudflare RayID/ParentRayID into OTel's
    native IDs — same shape libhoney's enrichment.py produced via event
    fields."""

    def __init__(self):
        self._fallback = RandomIdGenerator()
        self._next_trace_id = None
        self._next_span_id = None

    def set_next(self, trace_id=None, span_id=None):
        self._next_trace_id = trace_id
        self._next_span_id = span_id

    def generate_trace_id(self):
        if self._next_trace_id is not None:
            tid = self._next_trace_id
            self._next_trace_id = None
            return tid
        return self._fallback.generate_trace_id()

    def generate_span_id(self):
        if self._next_span_id is not None:
            sid = self._next_span_id
            self._next_span_id = None
            return sid
        return self._fallback.generate_span_id()


def _coerce_attribute_value(value):
    """
    Coerce a cloudflare log entry value into a type OTel will accept on a
    span attribute. Mirrors libhoney's "JSON-everything" behavior: dicts
    (ResponseHeaders, Cookies, RequestHeaders, JA4Signals, etc.) and
    mixed-type sequences become JSON strings so they land in Honeycomb
    as queryable-by-substring fields rather than being dropped with a
    per-attribute warning on every span.

    None values should be filtered by the caller.
    """
    if isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, (list, tuple)):
        # OTel accepts sequences of primitives directly. Drop Nones and
        # let them through; fall back to JSON for mixed-type sequences.
        if all(el is None or isinstance(el, (str, bool, int, float)) for el in value):
            return [el for el in value if el is not None]
    return orjson.dumps(value).decode("utf-8")


def is_already_processed(lock_bucket, object_name):
    try:
        return _processed_blob(lock_bucket, object_name).exists()
    except Exception as ex:
        raise RetriableError() from ex


def mark_as_processed(lock_bucket, object_name):
    blob = _processed_blob(lock_bucket, object_name)
    try:
        blob.upload_from_string(b"", if_generation_match=0)
    except PreconditionFailed:
        # Another invocation has already processed this but it failed to be
        # caught in the lock. Ignore
        pass
    except Exception:
        # We would really prefer to not raise at this point since we don't
        # want to retry since the data has already been processed, thus we pray
        # for this error to be transient and try again after a brief pause,
        # otherwise silently ignore the error as raising will duplicate data when
        # retried, but it not being marked as processed is only going to lead to
        # duplicate data if we are invoked more than once (like due to
        # at-least-once delivery guarantees).
        time.sleep(10)
        try:
            blob.upload_from_string(b'', if_generation_match=0)
        except Exception:
            pass


def _processed_blob(bucket, object_name):
    return bucket.blob("completed/%s" % object_name)


def download_file(bucket, object_name):
    blob = bucket.blob(object_name)
    local_path = "/tmp/" + os.path.basename(object_name)
    try:
        blob.download_to_filename(local_path, raw_download=True)
    except HTTPError as ex:
        raise RetriableError() from ex
    return local_path


def get_raw_file_entries(input_file):
    with gzip.open(input_file, "rt") as fh:
        yield from fh
