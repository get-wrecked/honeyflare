# honeyflare

Forward Cloudflare logs to Honeycomb as OpenTelemetry traces.

Each cloudflare log line becomes a span with `service.name=cloudflare`;
each invocation of the function itself emits a meta span with
`service.name=honeyflare`. When a log line has a `ParentRayID`, the span
inherits its parent's trace_id so worker-initiated subrequests end up in
the same trace as their originator.

Honeyflare assumes that your Cloudflare logs use the UnixNanos format for
timestamps, make sure you have that set before deploying Honeyflare.


## Routing

Honeyflare sends OTLP/HTTP traces to whatever `HONEYCOMB_API` points at
and does not send an ingest key itself. Expected deployment is behind a
[Refinery](https://github.com/honeycombio/refinery) configured with
`AccessKeys.SendKeyMode: missingonly`, which injects the Honeycomb ingest
key on egress. Honeycomb E&S (Environments & Services) routes events by
`service.name`, so no per-dataset config is needed on the honeyflare
side.


## Development

Run `./configure` to set up dependencies.

To run unit tests:

    $ ./test

To run the integration tests, set the environment variables
`HONEYFLARE_TEST_BUCKET` to a bucket you want to use and
`GOOGLE_APPLICATION_CREDENTIALS` to a path to the service account credentials
you want to use. Then:

    $ ./test -m integration


## Deployment

The file to be parsed is downloaded in its entirety to `/tmp`, which is
mounted as a tmpfs. Thus the function needs to have enough memory to
handle the biggest file you receive from Cloudflare.

```sh
$ gcloud functions deploy honeyflare \
    --entry-point main \
    --retry \
    --timeout 540 \
    --memory 256 \
    --service-account <account> \
    --env-vars-file sample-env.yml \
    --ingress-settings internal-only \
    --trigger-resource <bucket> \
    --trigger-event google.storage.object.finalize \
    --runtime python314
```


## License

This project is licensed under the Hippocratic License (a MIT derivative), and
is thus freely available to use for anyone that does not engage in human rights
violations as defined by UN's Universal Declaration of Human Rights or similar
applicable laws.
