"""
test_aws_connection.py

This checks if local Python can connect to AWS Kinesis and S3.
"""

import os
import boto3
from dotenv import load_dotenv


load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


def main():
    print("=" * 60)
    print("Testing AWS Connection")
    print("=" * 60)

    print(f"AWS Region: {AWS_REGION}")
    print(f"Kinesis Stream: {KINESIS_STREAM_NAME}")
    print(f"S3 Bucket: {S3_BUCKET_NAME}")

    kinesis = boto3.client("kinesis", region_name=AWS_REGION)
    s3 = boto3.client("s3", region_name=AWS_REGION)

    print("\nChecking Kinesis stream...")
    stream_response = kinesis.describe_stream_summary(
        StreamName=KINESIS_STREAM_NAME
    )

    stream_status = stream_response["StreamDescriptionSummary"]["StreamStatus"]
    print(f"Kinesis stream status: {stream_status}")

    print("\nChecking S3 bucket...")
    s3.head_bucket(Bucket=S3_BUCKET_NAME)
    print("S3 bucket found successfully.")

    print("\nAWS connection test completed successfully.")


if __name__ == "__main__":
    main()