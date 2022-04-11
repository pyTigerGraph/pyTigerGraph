import datetime
import os
import random
import string
from glob import glob
from os.path import join as pjoin
from urllib.parse import urlparse

import boto3
from google.cloud import storage as gcs


def upload_to_gcs(local_path: str, gcs_path: str, silent: bool = False) -> int:
    """Upload a file or a folder to Google cloud storage recursively.

    Args:
        local_path (str): Path to the local file or folder.
        gcs_path (str): Path to the Google cloud storage folder. Format: gs://bucket/path
    """
    url = urlparse(gcs_path)
    if url.scheme != "gs":
        raise ValueError(
            "Unrecognized GCS url. Expect format: gs://bucket/path")
    if not url.netloc:
        raise ValueError(
            "Cannot find bucket name. Expect format: gs://bucket/path")

    def recursive_upload(src_path: str, bucket, dest_path: str, silent: bool = False) -> int:
        if os.path.isfile(src_path):
            filename = os.path.basename(src_path)
            dest_file = pjoin(dest_path, filename)
            blob = bucket.blob(dest_file)
            blob.upload_from_filename(src_path)
            if not silent:
                print("Uploaded file {} to {}".format(src_path, dest_file))
            return [dest_file]
        uploaded_files: list = []
        dest_folder = pjoin(dest_path, os.path.basename(src_path))
        for path in glob(os.path.join(src_path, '*')):
            uploaded_files.extend(recursive_upload(
                path, bucket, dest_folder, uploaded_files))
        if not silent:
            print("Uploaded folder {} to {}".format(src_path, dest_folder))
        return uploaded_files

    client = gcs.Client()
    bucket = client.bucket(url.netloc)
    uploaded_files = recursive_upload(
        local_path, bucket, url.path.strip('/'), silent)
    uploaded_files = ["gs://{}/{}".format(url.netloc, i)
                      for i in uploaded_files]
    if not silent:
        print("Finished uploading {} files.".format(len(uploaded_files)))
    return uploaded_files


def upload_to_s3(local_path: str, s3_path: str,
                 aws_access_key_id: str, aws_secret_access_key: str,
                 silent: bool = True) -> int:
    """Upload a file to AWS S3.

    Args:
        local_path (str): Path to the local file.
        s3_path (str): Path to the S3 storage folder. Format: s3://bucket/path
    """
    url = urlparse(s3_path)
    if url.scheme != "s3":
        raise ValueError(
            "Unrecognized S3 url. Expect format: s3://bucket/path")
    if not url.netloc:
        raise ValueError(
            "Cannot find bucket name. Expect format: s3://bucket/path")

    if os.path.isfile(local_path):
        filename = os.path.basename(local_path)
        dest_file = pjoin(url.path.strip('/'), filename)
    else:
        raise NotImplementedError

    s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id,
                             aws_secret_access_key=aws_secret_access_key)
    response = s3_client.upload_file(local_path, url.netloc, dest_file)
    if not silent:
        print("Uploaded file {} to {}".format(local_path, dest_file))
    return ["s3://{}/{}".format(url.netloc, dest_file)]


def random_string(length: int = 1, chars: str = string.ascii_letters) -> str:
    return ''.join(random.choice(chars) for _ in range(length))


def today(fmt: str = "%Y%m%d") -> str:
    return datetime.date.today().strftime(fmt)
