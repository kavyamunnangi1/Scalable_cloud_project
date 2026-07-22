"""
upload_outputs_to_s3.py

Uploads local processed files and result files to Amazon S3.

This stores the local Lambda Architecture outputs into the correct
S3 folders for report/demo evidence.

Usage:
    python producer/upload_outputs_to_s3.py
"""

import os
from pathlib import Path

import boto3
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"

load_dotenv(dotenv_path=ENV_FILE)

AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


FILES_TO_UPLOAD = [
    # Processed dataset
    ("data/processed/combined_orders.csv", "processed/combined_orders.csv"),

    # Batch results
    ("results/batch_orders_by_restaurant.csv", "batch-results/batch_orders_by_restaurant.csv"),
    ("results/batch_orders_by_hour.csv", "batch-results/batch_orders_by_hour.csv"),
    ("results/batch_popular_items.csv", "batch-results/batch_popular_items.csv"),
    ("results/batch_revenue_by_restaurant.csv", "batch-results/batch_revenue_by_restaurant.csv"),
    ("results/batch_kitchen_workload.csv", "batch-results/batch_kitchen_workload.csv"),

    # Speed results
    ("results/speed_layer_results.csv", "speed-results/speed_layer_results.csv"),

    # Serving results
    ("results/serving_view.csv", "serving-results/serving_view.csv"),

    # Performance results
    ("results/performance_metrics.csv", "performance/performance_metrics.csv"),
    ("results/latency_vs_rate.png", "performance/latency_vs_rate.png"),
    ("results/throughput_vs_rate.png", "performance/throughput_vs_rate.png"),
    ("results/processing_time_vs_records.png", "performance/processing_time_vs_records.png"),
    ("results/speedup_vs_worker_count.png", "performance/speedup_vs_worker_count.png"),
]


def check_config():
    if not AWS_REGION:
        print("ERROR: AWS_REGION missing from .env")
        exit(1)

    if not S3_BUCKET_NAME:
        print("ERROR: S3_BUCKET_NAME missing from .env")
        exit(1)


def upload_file_to_s3(s3_client, local_path, s3_key):
    local_file = PROJECT_ROOT / local_path

    if not local_file.exists():
        print(f"SKIPPED: File not found: {local_path}")
        return False

    s3_client.upload_file(
        Filename=str(local_file),
        Bucket=S3_BUCKET_NAME,
        Key=s3_key
    )

    print(f"Uploaded: {local_path} -> s3://{S3_BUCKET_NAME}/{s3_key}")
    return True


def main():
    print("=" * 70)
    print("Uploading Local Pipeline Outputs to S3")
    print("=" * 70)

    check_config()

    print(f"AWS Region: {AWS_REGION}")
    print(f"S3 Bucket: {S3_BUCKET_NAME}")

    s3_client = boto3.client("s3", region_name=AWS_REGION)

    uploaded_count = 0
    skipped_count = 0

    for local_path, s3_key in FILES_TO_UPLOAD:
        success = upload_file_to_s3(s3_client, local_path, s3_key)

        if success:
            uploaded_count += 1
        else:
            skipped_count += 1

    print("\nUpload Summary:")
    print(f"Uploaded files: {uploaded_count}")
    print(f"Skipped files : {skipped_count}")

    print("\nS3 upload completed.")


if __name__ == "__main__":
    main()