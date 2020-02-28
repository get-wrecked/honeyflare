# honeyflare

Forward Cloudflare logs to Honeycomb


## Development

Run `./configure` to set up dependencies.

To run unit tests:

    $ ./test

To run the integration tests, set the environment variables
`HONEYFLARE_TEST_BUCKET` to a bucket you want to use and
`GOOGLE_APPLICATION_CREDENTIALS` to a path to the service account credentials
you want to use. Then:

    $ ./test -m integration


## License

This project is licensed under the Hippocratic License (a MIT derivative), and
is thus freely available to use for anyone that does not engage in human rights
violations as defined by UN's Universal Declaration of Human Rights or similar
applicable laws.
