import os
import random
import re
import string
from os.path import join as pjoin
from urllib.parse import urlparse
'''
import boto3
from google.cloud import storage as gcs


def download_from_gcs(gcs_path: str, local_path: str, silent: bool = False) -> str:
    """Download a file from Google cloud storage to local.

    Args:
        local_path (str): Path to the local folder.
        gcs_path (str): Path to the file on Google cloud storage. Format: gs://bucket/path
    """
    url = urlparse(gcs_path)
    if url.scheme != "gs":
        raise ValueError("Unrecognized GCS url. Expect format: gs://bucket/path")
    if not url.netloc:
        raise ValueError("Cannot find bucket name. Expect format: gs://bucket/path")

    client = gcs.Client()
    bucket = client.bucket(url.netloc)
    blob = bucket.blob(url.path.strip("/"))
    filename = os.path.basename(url.path)
    local_file = pjoin(local_path, filename)
    blob.download_to_filename(local_file)
    if not silent:
        print("Downloaded file {} to {}".format(gcs_path, local_file))

    return local_file


def download_from_s3(
    s3_path: str,
    local_path: str,
    aws_access_key_id: str,
    aws_secret_access_key: str,
    silent: bool = True,
) -> str:
    """Download a file from S3 to local.

    Args:
        local_path (str): Path to the local folder.
        s3_path (str): Path to the file on S3. Format: s3://bucket/path
    """
    url = urlparse(s3_path)
    if url.scheme != "s3":
        raise ValueError("Unrecognized S3 url. Expect format: s3://bucket/path")
    if not url.netloc:
        raise ValueError("Cannot find bucket name. Expect format: s3://bucket/path")

    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )
    filename = os.path.basename(url.path)
    local_file = pjoin(local_path, filename)
    s3.download_file(url.netloc, url.path.strip("/"), local_file)
    if not silent:
        print("Downloaded file {} to {}".format(s3_path, local_file))

    return local_file
'''

def random_string(length: int = 1, chars: str = string.ascii_letters) -> str:
    return "".join(random.choice(chars) for _ in range(length))


def validate_attributes_input(attributes: str) -> str:
    regex = re.compile(r"^((\w+(:\w+)?)(,\w+(:\w+)?)*)?$")
    if regex.match(attributes) is None:
        raise ValueError(
            "Illegal characters in {}. Required format: 'attr1:type1,attr2:type2,...' where type can be omitted with colon.".format(
                attributes
            )
        )
    return attributes
