"""
stream_producer.py

Simulates a live restaurant order event stream by replaying rows from
data/processed/combined_orders.csv one by one.

It can run in two modes:

1. Local mode:
   Prints events locally and optionally saves them to a JSONL file.

2. AWS mode:
   Sends events to AWS Kinesis Data Streams using boto3.

Usage:
    python producer/stream_producer.py --limit 20 --rate 2
    python producer/stream_producer.py --limit 20 --rate 2 --output results/sample_stream_events.jsonl
    python producer/stream_producer.py --limit 20 --rate 2 --aws
"""

import argparse
import json
import os
import time
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv


# -------------------------------------------------------
# File paths
# -------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

DEFAULT_INPUT = "data/processed/combined_orders.csv"


# -------------------------------------------------------
# Load AWS config from .env
# -------------------------------------------------------
load_dotenv(dotenv_path=ENV_FILE)

AWS_REGION = os.getenv("AWS_REGION")
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")


def load_dataset(filepath):
    """
    Load the prepared combined orders CSV.
    """
    if not os.path.exists(filepath):
        print(f"ERROR: Input file not found: {filepath}")
        print("Please run: python data/prepare_dataset.py")
        exit(1)

    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} records from {filepath}")
    return df


def row_to_event(row):
    """
    Convert a DataFrame row into a clean JSON event.
    """
    return {
        "event_id": int(row.get("event_id", 0)),
        "restaurant_id": str(row.get("restaurant_id", "")),
        "order_id": str(row.get("order_id", "")),
        "event_time": str(row.get("event_time", "")),
        "order_date": str(row.get("order_date", "")),
        "order_hour": int(row.get("order_hour", 0)),
        "day_name": str(row.get("day_name", "")),
        "item_name": str(row.get("item_name", "")),
        "quantity": int(row.get("quantity", 0)),
        "product_price": float(row.get("product_price", 0.0)),
        "total_products": int(row.get("total_products", 0)),
        "order_item_value": float(row.get("order_item_value", 0.0)),
        "kitchen_station": str(row.get("kitchen_station", "")),
    }


def create_kinesis_client():
    """
    Create AWS Kinesis client.
    """
    if not AWS_REGION or not KINESIS_STREAM_NAME:
        print("ERROR: AWS_REGION or KINESIS_STREAM_NAME missing from .env")
        exit(1)

    return boto3.client("kinesis", region_name=AWS_REGION)


def send_to_kinesis(kinesis_client, event):
    """
    Send one event to AWS Kinesis Data Streams.
    """
    response = kinesis_client.put_record(
        StreamName=KINESIS_STREAM_NAME,
        Data=json.dumps(event),
        PartitionKey=event["restaurant_id"]
    )

    return response


def main():
    parser = argparse.ArgumentParser(
        description="Restaurant Order Stream Producer"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of records to stream"
    )

    parser.add_argument(
        "--rate",
        type=float,
        default=5.0,
        help="Records per second"
    )

    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Optional local JSONL output file"
    )

    parser.add_argument(
        "--aws",
        action="store_true",
        help="Send events to AWS Kinesis"
    )

    args = parser.parse_args()

    if args.rate <= 0:
        print("ERROR: --rate must be greater than 0")
        exit(1)

    delay = 1.0 / args.rate

    df = load_dataset(DEFAULT_INPUT)
    df = df.head(args.limit)

    print("\nStarting stream producer...")
    print(f"Records to stream: {len(df)}")
    print(f"Target rate: {args.rate} records/second")
    print(f"AWS mode: {'ON' if args.aws else 'OFF'}")

    output_file = None

    if args.output:
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        output_file = open(args.output, "w", encoding="utf-8")
        print(f"Local output file: {args.output}")

    kinesis_client = None

    if args.aws:
        print(f"Kinesis stream: {KINESIS_STREAM_NAME}")
        print(f"AWS region: {AWS_REGION}")
        kinesis_client = create_kinesis_client()

    print("-" * 80)

    start_time = time.time()
    sent_count = 0
    failed_count = 0

    for _, row in df.iterrows():
        event = row_to_event(row.to_dict())
        event_json = json.dumps(event)

        print(f"[Event {sent_count + 1:04d}] {event_json}")

        if output_file:
            output_file.write(event_json + "\n")

        if args.aws:
            try:
                response = send_to_kinesis(kinesis_client, event)
                shard_id = response.get("ShardId", "unknown")
                sequence_number = response.get("SequenceNumber", "unknown")
                print(f"  -> Sent to Kinesis | Shard: {shard_id} | Sequence: {sequence_number}")
            except Exception as error:
                failed_count += 1
                print(f"  -> ERROR sending to Kinesis: {error}")

        sent_count += 1
        time.sleep(delay)

    if output_file:
        output_file.close()

    end_time = time.time()
    total_time = end_time - start_time
    actual_rate = sent_count / total_time if total_time > 0 else 0

    print("-" * 80)
    print("Stream complete!")
    print(f"Records processed : {sent_count}")
    print(f"Failed records    : {failed_count}")
    print(f"Total time        : {total_time:.2f} seconds")
    print(f"Target rate       : {args.rate:.2f} records/sec")
    print(f"Actual rate       : {actual_rate:.2f} records/sec")

    if args.output:
        print(f"Local output saved to: {args.output}")

    if args.aws:
        print(f"Events sent to AWS Kinesis stream: {KINESIS_STREAM_NAME}")


if __name__ == "__main__":
    main()