import ipaddress
import uuid

from .urlshape import urlshape


def enrich_entry(entry, path_patterns, query_param_filter):
    '''
    :param entry: A dictionary with the log entry fields.
    :param path_patterns: A list of `.urlshape.Pattern` for known path patterns
        to parse.
    '''
    # Which fields are included will vary depending on the logging config, thus don't
    # assume anything
    edge_end_timestamp = entry.get('EdgeEndTimestamp')
    edge_start_timestamp = entry.get('EdgeStartTimestamp')
    if edge_end_timestamp is not None and edge_start_timestamp is not None:
        enrich_duration(entry, edge_start_timestamp, edge_end_timestamp)

    origin_response_time_ns = entry.get('OriginResponseTime')
    if origin_response_time_ns:
        enrich_origin_response_time(entry, origin_response_time_ns)

    client_ip = entry.get('ClientIP')
    if client_ip is not None:
        enrich_client_ip(entry, client_ip)

    client_request_uri = entry.get('ClientRequestURI')
    if client_request_uri is not None:
        enrich_urlshape(entry, client_request_uri, path_patterns, query_param_filter)

    # Cloudflare says the RayId should be unique, thus it should be unique also when padded to form a full uuid
    ray_id = entry.get('RayId')
    if ray_id is not None:
        entry['trace.span_id'] = uuid.UUID('0000000000000000' + ray_id)
    else:
        entry['trace.span_id'] = str(uuid.uuid4())

    parent_ray_id = entry.get('ParentRayId')
    if parent_ray_id is not None and parent_ray_id != '00':
        entry['trace.trace_id'] = uuid.UUID('0000000000000000' + parent_ray_id)
    else:
        entry['trace.trace_id'] = str(uuid.uuid4())

    # Required fields for otel compatibility
    entry['service.name'] = 'cloudflare'
    entry['name'] = 'HTTP %s' % entry.get('ClientRequestMethod', 'N/A')


def enrich_duration(entry, start_ns, end_ns):
    duration_ms = (end_ns - start_ns)/1e6
    entry['DurationSeconds'] = duration_ms/1000
    entry['DurationMs'] = duration_ms


def enrich_origin_response_time(entry, origin_response_time_ns):
    entry['OriginResponseTimeSeconds'] = origin_response_time_ns/1e9
    entry['OriginResponseTimeMs'] = origin_response_time_ns/1e6


def enrich_client_ip(entry, client_ip):
    parsed_ip = ipaddress.ip_address(client_ip)
    entry['ClientIPVersion'] = parsed_ip.version


def enrich_urlshape(entry, client_request_uri, path_patterns, query_param_filter):
    url_shape = urlshape(client_request_uri, path_patterns, query_param_filter)
    entry['Path'] = url_shape.path
    entry['PathShape'] = url_shape.path_shape
    entry['Query'] = url_shape.query
    entry['QueryShape'] = url_shape.query_shape
    entry['UriShape'] = url_shape.uri_shape
    for path_param, value in url_shape.path_params.items():
        entry['Path_' + path_param] = value
    for query_param, value in url_shape.query_params.items():
        entry['Query_' + query_param] = value
