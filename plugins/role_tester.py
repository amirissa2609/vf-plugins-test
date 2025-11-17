import boto3
import json

def process_scheduled_call(influxdb3_local, call_time, args=None):

    if args and "s3bucket" in args:
        s3_bucket = str(args["s3bucket"])
    else:
        influxdb3_local.info("s3bucket not supplied in args")

    if args and "s3file" in args:
        s3_file = str(args["s3file"])
    else:
        influxdb3_local.info("s3file not supplied in args")


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

    # Upload empty file
    s3.put_object(Bucket=s3_bucket, Key=s3_file, Body=b"")

    influxdb3_local.info(f"Uploaded empty file to s3://{s3_bucket}/{s3_file}")


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

