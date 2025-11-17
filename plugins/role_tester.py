import boto3
import json
from urllib.parse import urlparse

def process_scheduled_call(influxdb3_local, call_time, args=None):

    if args and "s3uri" in args:
        s3_uri = str(args["s3uri"])
    else:
        influxdb3_local.info("s3uri not supplied in args")

    if args and "credsfile" in args:
        creds_file = str(args["credsfile"])
    else:
        influxdb3_local.info("credsfile not supplied in args")

    # Load credentials from JSON file
    creds = load_credentials_json(creds_file)

    # Create boto3 client with session token
    s3 = boto3.client(
        "s3",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        aws_session_token=creds["aws_session_token"],
    )

    # Parse S3 URI into bucket + key
    bucket, key = parse_s3_uri(s3_uri)

    # Upload empty file
    s3.put_object(Bucket=bucket, Key=key, Body=b"")

    influxdb3_local.info(f"Uploaded empty file to s3://{bucket}/{key}")


def load_credentials_json(creds_path):
    """Load AWS credentials from a JSON file with keys:
       aws_access_key_id, aws_secret_access_key, aws_session_token
    """
    data = ""
    with open(creds_path, "r") as f:
        data = json.load(f)

    required = ["aws_access_key_id", "aws_secret_access_key", "aws_session_token"]
    for key in required:
        if key not in data:
            influxdb3_local.info(f"Missing required key '{key}' in credentials JSON.")

    return data


def parse_s3_uri(s3_uri):
    """Parse an S3 URI like s3://my-bucket/path/to/file.txt."""
    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        raise ValueError("S3 URI must start with s3://")

    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    return bucket, key

