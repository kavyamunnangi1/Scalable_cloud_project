"""
benchmark_runner.py

Performance benchmarking script for the restaurant order stream project.

This script tests local processing performance for different:
- Streaming rates
- Record limits
- Worker counts

It measures:
- CPU processing time
- CPU throughput
- Estimated stream time
- Estimated latency
- Speedup compared with 1 worker

Outputs:
- results/performance_metrics.csv
- results/latency_vs_rate.png
- results/throughput_vs_rate.png
- results/processing_time_vs_records.png
- results/speedup_vs_worker_count.png

Usage:
    python performance/benchmark_runner.py
"""

import os
import time
import json
from concurrent.futures import ThreadPoolExecutor

import pandas as pd
import matplotlib.pyplot as plt


# -------------------------------------------------------
# File paths
# -------------------------------------------------------
INPUT_FILE = "data/processed/combined_orders.csv"
RESULTS_DIR = "results"


# -------------------------------------------------------
# Benchmark settings
# -------------------------------------------------------
RATES_TO_TEST = [10, 50, 100]          # records per second
LIMITS_TO_TEST = [500, 1000, 2000]    # number of records
WORKERS_TO_TEST = [1, 2, 4]           # simulated worker counts


def load_data(file_path):
    """
    Load the prepared combined orders dataset.
    """
    if not os.path.exists(file_path):
        print(f"ERROR: File not found: {file_path}")
        print("Please run: python data/prepare_dataset.py")
        exit(1)

    df = pd.read_csv(file_path)
    print(f"Loaded {len(df)} records from {file_path}")
    return df


def event_to_json(record):
    """
    Convert one record into a JSON event.
    This simulates the work done by a stream producer.
    """
    event = {
        "event_id": record.get("event_id"),
        "restaurant_id": record.get("restaurant_id"),
        "order_id": record.get("order_id"),
        "event_time": record.get("event_time"),
        "item_name": record.get("item_name"),
        "quantity": record.get("quantity"),
        "product_price": record.get("product_price"),
        "order_item_value": record.get("order_item_value"),
        "kitchen_station": record.get("kitchen_station"),
    }

    return json.dumps(event, default=str)


def process_chunk(records):
    """
    Process a chunk of records.
    Returns how many records were processed.
    """
    count = 0

    for record in records:
        _ = event_to_json(record)
        count += 1

    return count


def split_into_chunks(records, number_of_chunks):
    """
    Split records into approximately equal chunks.
    """
    chunk_size = max(1, len(records) // number_of_chunks)
    chunks = []

    for start in range(0, len(records), chunk_size):
        end = start + chunk_size
        chunks.append(records[start:end])

    return chunks


def run_processing_test(records, worker_count):
    """
    Process records using 1 or more workers.

    For 1 worker:
        Sequential processing is used.

    For more workers:
        ThreadPoolExecutor is used to simulate parallel worker processing.
    """
    start_time = time.perf_counter()

    if worker_count == 1:
        processed_records = process_chunk(records)

    else:
        chunks = split_into_chunks(records, worker_count)

        processed_records = 0

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            results = executor.map(process_chunk, chunks)

            for result in results:
                processed_records += result

    end_time = time.perf_counter()

    cpu_time_seconds = end_time - start_time
    cpu_throughput = processed_records / cpu_time_seconds if cpu_time_seconds > 0 else 0
    avg_processing_latency_ms = (cpu_time_seconds / processed_records) * 1000 if processed_records > 0 else 0

    return {
        "records_processed": processed_records,
        "cpu_time_seconds": cpu_time_seconds,
        "cpu_throughput": cpu_throughput,
        "avg_processing_latency_ms": avg_processing_latency_ms,
    }


def run_benchmark(df):
    """
    Run benchmark tests for all rates, limits and worker counts.
    """
    results = []
    run_number = 0

    total_runs = len(RATES_TO_TEST) * len(LIMITS_TO_TEST) * len(WORKERS_TO_TEST)

    print(f"\nTotal benchmark runs: {total_runs}")
    print("-" * 70)

    for limit in LIMITS_TO_TEST:
        limited_df = df.head(limit)
        records = limited_df.to_dict(orient="records")

        for rate in RATES_TO_TEST:
            baseline_time = None

            for worker_count in WORKERS_TO_TEST:
                run_number += 1

                print(
                    f"Run {run_number:02d}/{total_runs}: "
                    f"limit={limit}, rate={rate} rec/s, workers={worker_count} ... ",
                    end=""
                )

                metrics = run_processing_test(records, worker_count)

                if worker_count == 1:
                    baseline_time = metrics["cpu_time_seconds"]
                    speedup = 1.0
                else:
                    speedup = baseline_time / metrics["cpu_time_seconds"] if metrics["cpu_time_seconds"] > 0 else 0

                estimated_stream_time = metrics["records_processed"] / rate
                target_interval_latency_ms = (1 / rate) * 1000

                estimated_total_latency_ms = (
                    target_interval_latency_ms + metrics["avg_processing_latency_ms"]
                )

                row = {
                    "limit_records": limit,
                    "rate_records_per_second": rate,
                    "worker_count": worker_count,
                    "records_processed": metrics["records_processed"],
                    "cpu_time_seconds": round(metrics["cpu_time_seconds"], 4),
                    "cpu_throughput_records_per_second": round(metrics["cpu_throughput"], 2),
                    "avg_processing_latency_ms": round(metrics["avg_processing_latency_ms"], 4),
                    "estimated_stream_time_seconds": round(estimated_stream_time, 4),
                    "estimated_total_latency_ms": round(estimated_total_latency_ms, 4),
                    "speedup_vs_one_worker": round(speedup, 4),
                }

                results.append(row)

                print(
                    f"CPU time={row['cpu_time_seconds']}s | "
                    f"throughput={row['cpu_throughput_records_per_second']} rec/s | "
                    f"latency={row['estimated_total_latency_ms']} ms | "
                    f"speedup={row['speedup_vs_one_worker']}"
                )

    return pd.DataFrame(results)


def save_metrics(metrics_df):
    """
    Save benchmark metrics to CSV.
    """
    os.makedirs(RESULTS_DIR, exist_ok=True)

    output_path = os.path.join(RESULTS_DIR, "performance_metrics.csv")
    metrics_df.to_csv(output_path, index=False)

    print(f"\nPerformance metrics saved to: {output_path}")


def plot_latency_vs_rate(metrics_df):
    """
    Plot estimated latency against streaming rate.
    Uses 1 worker results for a simple graph.
    """
    output_path = os.path.join(RESULTS_DIR, "latency_vs_rate.png")

    plot_df = metrics_df[metrics_df["worker_count"] == 1]

    plt.figure(figsize=(8, 5))

    for limit in plot_df["limit_records"].unique():
        subset = plot_df[plot_df["limit_records"] == limit].sort_values("rate_records_per_second")
        plt.plot(
            subset["rate_records_per_second"],
            subset["estimated_total_latency_ms"],
            marker="o",
            label=f"{limit} records"
        )

    plt.title("Latency vs Streaming Rate")
    plt.xlabel("Streaming Rate (records/second)")
    plt.ylabel("Estimated Total Latency (ms)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"Graph saved: {output_path}")


def plot_throughput_vs_rate(metrics_df):
    """
    Plot CPU throughput against streaming rate.
    Uses 1 worker results for a simple graph.
    """
    output_path = os.path.join(RESULTS_DIR, "throughput_vs_rate.png")

    plot_df = metrics_df[metrics_df["worker_count"] == 1]

    plt.figure(figsize=(8, 5))

    for limit in plot_df["limit_records"].unique():
        subset = plot_df[plot_df["limit_records"] == limit].sort_values("rate_records_per_second")
        plt.plot(
            subset["rate_records_per_second"],
            subset["cpu_throughput_records_per_second"],
            marker="s",
            label=f"{limit} records"
        )

    plt.title("CPU Throughput vs Streaming Rate")
    plt.xlabel("Streaming Rate (records/second)")
    plt.ylabel("CPU Throughput (records/second)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"Graph saved: {output_path}")


def plot_processing_time_vs_records(metrics_df):
    """
    Plot processing time against number of records.
    Uses rate 50 records/second for a clean comparison.
    """
    output_path = os.path.join(RESULTS_DIR, "processing_time_vs_records.png")

    plot_df = metrics_df[metrics_df["rate_records_per_second"] == 50]

    plt.figure(figsize=(8, 5))

    for workers in plot_df["worker_count"].unique():
        subset = plot_df[plot_df["worker_count"] == workers].sort_values("limit_records")
        plt.plot(
            subset["limit_records"],
            subset["cpu_time_seconds"],
            marker="^",
            label=f"{workers} worker(s)"
        )

    plt.title("Processing Time vs Number of Records")
    plt.xlabel("Number of Records")
    plt.ylabel("CPU Processing Time (seconds)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"Graph saved: {output_path}")


def plot_speedup_vs_worker_count(metrics_df):
    """
    Plot average speedup for each worker count.
    """
    output_path = os.path.join(RESULTS_DIR, "speedup_vs_worker_count.png")

    speedup_df = metrics_df.groupby("worker_count")["speedup_vs_one_worker"].mean().reset_index()

    plt.figure(figsize=(8, 5))
    plt.plot(
        speedup_df["worker_count"],
        speedup_df["speedup_vs_one_worker"],
        marker="o"
    )

    plt.title("Speedup vs Worker Count")
    plt.xlabel("Worker Count")
    plt.ylabel("Average Speedup Compared with 1 Worker")
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print(f"Graph saved: {output_path}")


def main():
    print("=" * 70)
    print("Performance Benchmark Runner")
    print("=" * 70)

    df = load_data(INPUT_FILE)

    metrics_df = run_benchmark(df)

    save_metrics(metrics_df)

    print("\nFull Metrics Table:")
    print(metrics_df.to_string(index=False))

    print("\nGenerating performance graphs...")
    plot_latency_vs_rate(metrics_df)
    plot_throughput_vs_rate(metrics_df)
    plot_processing_time_vs_records(metrics_df)
    plot_speedup_vs_worker_count(metrics_df)

    print("\nBenchmark completed successfully.")
    print(f"All outputs saved in: {RESULTS_DIR}/")
    print("\nNote: This is a local benchmark. AWS Kinesis/S3/EC2 or EMR benchmarking will be added later.")


if __name__ == "__main__":
    main()