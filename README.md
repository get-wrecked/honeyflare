# honeyflare

Forward Cloudflare logs to Honeycomb

Honeyflare assumes that your Cloudflare logs uses the UnixNanos format for
timestamps, make sure you have that set before deploying Honeyflare.


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

The file to parsed is currently downloaded in it's entirety to `/tmp`, which is
mounted as a tmpfs. Thus the function needs to have enough memory to handle the
biggest file you receive from Cloudflare.

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
    --runtime python37
```


## License

This project is licensed under the Hippocratic License (a MIT derivative), and
is thus freely available to use for anyone that does not engage in human rights
violations as defined by UN's Universal Declaration of Human Rights or similar
applicable laws.
