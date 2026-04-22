import gzip
import os
import time

from google.api_core.exceptions import PreconditionFailed
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
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
    tracer, provider = create_otel_tracer(
        service_name="cloudflare",
        honeycomb_api=honeycomb_api,
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
                context = _build_trace_context(entry.get("RayID"), entry.get("ParentRayID"))

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
                    # Filter None upfront to avoid OTel warning on each
                    # dropped attribute. Non-primitive values (shouldn't
                    # happen for cloudflare logs) will be dropped with a
                    # warning by OTel, which is the right signal.
                    span.set_attributes(
                        {k: v for k, v in entry.items() if v is not None}
                    )
                finally:
                    span.end(end_time=start_time_ns)
                total_events += 1

            os.remove(local_path)
            mark_as_processed(lock_bucket, object_name)
    finally:
        provider.shutdown()
    return total_events


def create_otel_tracer(service_name, honeycomb_api):
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

    Returns (tracer, provider). Callers should call provider.shutdown() at
    the end of the scope to flush buffered spans.
    """
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.version": __version__,
        }
    )
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(
        endpoint="%s/v1/traces" % honeycomb_api.rstrip("/"),
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    return provider.get_tracer("honeyflare"), provider


def _build_trace_context(ray_id, parent_ray_id):
    """
    Build an OTel context for a cloudflare log line so worker-initiated
    subrequests share a trace with their parent worker request.

    Cloudflare RayIDs are 16 hex chars (64 bits). OTel trace IDs are 128
    bits, so the 64-bit int sits in the low half (high half zero) — same
    shape as the old libhoney `uuid_from_ray_id` padding. A non-recording
    parent span carries the parent-derived trace and span IDs; the child
    span (the one we record) inherits trace_id and sets parent_span_id.

    Standalone requests (no ParentRayID, or "00" meaning none) return an
    empty context so OTel generates both trace_id and span_id randomly.
    RayID is still available as a span attribute for lookups.
    """
    if not ray_id or not parent_ray_id or parent_ray_id == "00":
        return trace.Context()

    parent_span_id = _ray_to_int(parent_ray_id)
    trace_id = _ray_to_int(parent_ray_id)

    parent_ctx = SpanContext(
        trace_id=trace_id,
        span_id=parent_span_id,
        is_remote=True,
        trace_flags=TraceFlags(TraceFlags.SAMPLED),
    )
    return trace.set_span_in_context(NonRecordingSpan(parent_ctx))


def _ray_to_int(ray_id):
    return int(ray_id, 16)


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
