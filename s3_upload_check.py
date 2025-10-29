"""s3_upload_check.py

Simple script to upload a file to S3 and verify it exists.
Usage examples are in README.md.

This script expects AWS credentials to be available via environment variables
(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) or an AWS named profile.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from typing import Optional

import boto3
from botocore.exceptions import ClientError


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def upload_file(bucket: str, key: str, filename: str, region: Optional[str] = None, profile: Optional[str] = None) -> bool:
    """Upload a local file to S3.

    Returns True if upload succeeded, False otherwise.
    """
    if not os.path.exists(filename):
        logger.error("Local file '%s' does not exist.", filename)
        return False

    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3", region_name=region)

    try:
        s3.upload_file(filename, bucket, key)
        logger.info("Uploaded '%s' to 's3://%s/%s'", filename, bucket, key)
        return True
    except ClientError as e:
        logger.error("Failed to upload file: %s", e)
        return False


def object_exists(bucket: str, key: str, region: Optional[str] = None, profile: Optional[str] = None) -> bool:
    """Return True if the object exists in S3, False otherwise."""
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    s3 = session.client("s3", region_name=region)

    try:
        s3.head_object(Bucket=bucket, Key=key)
        logger.info("Object exists: s3://%s/%s", bucket, key)
        return True
    except ClientError as e:
        code = getattr(e, "response", {}).get("Error", {}).get("Code")
        if code in ("404", "NotFound"):
            logger.info("Object not found: s3://%s/%s", bucket, key)
            return False
        logger.error("Error checking object existence: %s", e)
        return False


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Upload file to S3 and verify it exists.")
    p.add_argument("--bucket", required=True, help="S3 bucket name")
    p.add_argument("--key", required=True, help="S3 object key (path in bucket)")
    p.add_argument("--file", required=True, help="Local file path to upload")
    p.add_argument("--region", help="AWS region (optional)")
    p.add_argument("--profile", help="AWS named profile to use (optional)")
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = parse_args(argv)

    ok = upload_file(args.bucket, args.key, args.file, region=args.region, profile=args.profile)
    if not ok:
        logger.error("Upload failed; exiting with code 2")
        return 2

    exists = object_exists(args.bucket, args.key, region=args.region, profile=args.profile)
    if exists:
        logger.info("Verified: object exists in S3.")
        return 0
    else:
        logger.error("Verification failed: object not found after upload.")
        return 3


if __name__ == "__main__":
    sys.exit(main())
