import boto3

def process_scheduled_call(influxdb3_local, call_time, args=None):

    if args and "access_key" in args:
        access_key = str(args["access_key"])
    else:
        influxdb3_local.info("access_key not supplied in args")

    if args and "secret_key_id" in args:
        secret_key_id = str(args["secret_key_id"])
    else:
        influxdb3_local.info("s3file not supplied in args")

    if args and "access_token" in args:
        access_token = str(args["access_token"])
    else:
        influxdb3_local.info("credsfile not supplied in args")

    if args and "s3_bucket" in args:
        s3_bucket = str(args["s3_bucket"])
    else:
        influxdb3_local.info("credsfile not supplied in args")

    # Create boto3 client with session token
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key_id,
        aws_session_token=access_token,
    )

    # Upload empty file
    s3.put_object(Bucket=s3_bucket, Key="test.txt", Body=b"")

    influxdb3_local.info(f"Uploaded empty file to s3://{s3_bucket}/test.txt")


