"""
kinesis_to_s3_consumer.py

Reads restaurant order events from AWS Kinesis Data Streams
and saves them into Amazon S3 raw/ folder as a JSONL file.

This proves the AWS ingestion and storage pipeline:

Local Producer -> Kinesis Data Stream -> S3 raw storage

Usage:
    python producer/kinesis_to_s3_consumer.py --max-records 20
"""

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path

import boto3
from dotenv import load_dotenv


# -------------------------------------------------------
# Load .env from project root
# -------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_FILE)

AWS_REGION = os.getenv("AWS_REGION")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


def check_config():
    """
    Check that required AWS values are present.
    """
    missing = []

    if not AWS_REGION:
        missing.append("AWS_REGION")

    if not KINESIS_STREAM_NAME:
        missing.append("KINESIS_STREAM_NAME")

    if not S3_BUCKET_NAME:
        missing.append("S3_BUCKET_NAME")

    if missing:
        print("ERROR: Missing values in .env file:")
        for item in missing:
            print(f"  - {item}")
        exit(1)


def create_clients():
    """
    Create Kinesis and S3 clients.
    """
    kinesis = boto3.client("kinesis", region_name=AWS_REGION)
    s3 = boto3.client("s3", region_name=AWS_REGION)

    return kinesis, s3


def get_first_shard_id(kinesis_client):
    """
    Get the first shard id from the Kinesis stream.
    """
    response = kinesis_client.describe_stream(StreamName=KINESIS_STREAM_NAME)
    shards = response["StreamDescription"]["Shards"]

    if not shards:
        print("ERROR: No shards found in Kinesis stream.")
        exit(1)

    shard_id = shards[1]["ShardId"]
    return shard_id


def read_records_from_kinesis(kinesis_client, shard_id, max_records):
    """
    Read records from Kinesis using TRIM_HORIZON.

    TRIM_HORIZON starts reading from the oldest available records in the stream.
    """
    print(f"Reading from shard: {shard_id}")

    iterator_response = kinesis_client.get_shard_iterator(
        StreamName=KINESIS_STREAM_NAME,
        ShardId=shard_id,
        ShardIteratorType="TRIM_HORIZON"
    )

    shard_iterator = iterator_response["ShardIterator"]

    collected_records = []
    empty_attempts = 0

    while len(collected_records) < max_records and empty_attempts < 5:
        records_response = kinesis_client.get_records(
            ShardIterator=shard_iterator,
            Limit=max_records
        )

        records = records_response["Records"]
        shard_iterator = records_response["NextShardIterator"]

        if not records:
            empty_attempts += 1
            print("No records found yet. Waiting...")
            time.sleep(2)
            continue

        for record in records:
            data_bytes = record["Data"]
            data_text = data_bytes.decode("utf-8")

            try:
                data_json = json.loads(data_text)
            except json.JSONDecodeError:
                data_json = {"raw_data": data_text}

            collected_records.append(data_json)

            if len(collected_records) >= max_records:
                break

        print(f"Collected records so far: {len(collected_records)}")

    return collected_records


def upload_records_to_s3(s3_client, records):
    """
    Upload consumed Kinesis records to S3 raw/ folder as JSONL.
    """
    if not records:
        print("No records to upload to S3.")
        return None

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_key = f"raw/kinesis_records_{timestamp}.jsonl"

    jsonl_data = "\n".join(json.dumps(record) for record in records)

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=s3_key,
        Body=jsonl_data.encode("utf-8"),
        ContentType="application/json"
    )

    return s3_key


def save_records_locally(records):
    """
    Also save a local copy inside results/ for easy checking.
    """
    if not records:
        return None

    results_dir = PROJECT_ROOT / "results"
    results_dir.mkdir(exist_ok=True)

    local_file = results_dir / "kinesis_consumed_records.jsonl"

    with open(local_file, "w", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record) + "\n")

    return local_file


def main():
    parser = argparse.ArgumentParser(
        description="Read records from Kinesis and store them in S3"
    )

    parser.add_argument(
        "--max-records",
        type=int,
        default=20,
        help="Maximum number of records to read from Kinesis"
    )

    args = parser.parse_args()

    print("=" * 70)
    print("Kinesis to S3 Consumer")
    print("=" * 70)

    check_config()

    print(f"AWS Region: {AWS_REGION}")
    print(f"Kinesis Stream: {KINESIS_STREAM_NAME}")
    print(f"S3 Bucket: {S3_BUCKET_NAME}")

    kinesis_client, s3_client = create_clients()

    shard_id = get_first_shard_id(kinesis_client)

    records = read_records_from_kinesis(
        kinesis_client=kinesis_client,
        shard_id=shard_id,
        max_records=args.max_records
    )

    print(f"\nTotal records consumed from Kinesis: {len(records)}")

    local_file = save_records_locally(records)

    if local_file:
        print(f"Local copy saved to: {local_file}")

    s3_key = upload_records_to_s3(s3_client, records)

    if s3_key:
        print(f"Uploaded records to S3:")
        print(f"s3://{S3_BUCKET_NAME}/{s3_key}")

    print("\nKinesis to S3 consumer completed successfully.")


if __name__ == "__main__":
    main()