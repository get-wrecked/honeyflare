#!./venv/bin/python

import argparse
import zipfile
import shutil
import os
import subprocess
import sys
import time

from google.cloud import storage


def main():
    args = get_args()
    clean()
    version = get_version()
    artifact = build_artifact(version)
    if not args.build_only:
        release_url = upload(args.bucket, artifact, version)
        print(release_url)


def clean():
    try:
        shutil.rmtree("artifacts")
    except FileNotFoundError:
        pass

    for dirname, dirnames, filenames in os.walk("honeyflare"):
        for index, directory in enumerate(dirnames):
            if directory == "__pycache__":
                shutil.rmtree(os.path.join(dirname, directory))
                del dirnames[index]


def get_version():
    return (
        subprocess.check_output(
            [
                "git",
                "rev-parse",
                "--short=20",
                "HEAD",
            ]
        )
        .decode("utf-8")
        .strip()
    )


def build_artifact(version):
    os.mkdir("artifacts")
    artifact_path = "artifacts/honeyflare.zip"
    with zipfile.ZipFile(artifact_path, "w") as fh:
        fh.write("main.py")
        fh.write("requirements.txt")
        for dirname, dirnames, filenames in os.walk("honeyflare"):
            for filename in filenames:
                path = os.path.join(dirname, filename)
                if path == 'honeyflare/version.py':
                    # Create a custom version spec with the git treeish appended
                    # to the base version in version.py
                    with open(path) as version_fh:
                        base_version = version_fh.readlines()[0].split('=', 1)[1].strip().strip('"')
                    info = zipfile.ZipInfo(path, time.localtime(time.time())[:6])
                    info.external_attr = 0o10644 << 16
                    fh.writestr(info, '__version__ = "%s-%s"\n' % (base_version, version))
                    continue
                fh.write(path)
    return artifact_path


def upload(bucket_name, artifact_path, version):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob("honeyflare/honeyflare-%s.zip" % version)
    with open(artifact_path, "rb") as fh:
        blob.upload_from_file(
            fh,
            content_type="application/zip",
        )
    return blob.public_url


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "bucket", help="The GCS bucket to upload the artifact to", nargs="?"
    )
    parser.add_argument(
        "--build-only",
        action="store_true",
        help="Don't upload the artifact anywhere, just build it",
    )
    args = parser.parse_args()
    if not args.build_only and not args.bucket:
        sys.stderr.write("Must specify a bucket if not using --build-only\n")
        sys.exit(1)

    return args


if __name__ == "__main__":
    main()
