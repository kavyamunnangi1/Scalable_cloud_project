"""
combine_results.py

The Serving Layer combines the Batch Layer and Speed Layer results.

Batch Layer:
    Gives full historical kitchen workload.

Speed Layer:
    Gives recent window-based kitchen workload.

Output:
    results/serving_view.csv

Usage:
    python serving_layer/combine_results.py
"""

import os
import pandas as pd


# -------------------------------------------------------
# File paths
# -------------------------------------------------------
BATCH_WORKLOAD_FILE = "results/batch_kitchen_workload.csv"
SPEED_RESULTS_FILE = "results/speed_layer_results.csv"
OUTPUT_FILE = "results/serving_view.csv"


def load_file(file_path, file_description):
    """
    Load a CSV file and show a clear message if the file is missing.
    """
    if not os.path.exists(file_path):
        print(f"ERROR: {file_description} not found: {file_path}")
        print("Please run the required previous layer first.")
        exit(1)

    df = pd.read_csv(file_path)
    print(f"Loaded {file_description}: {len(df)} rows")
    return df


def prepare_speed_summary(speed_df):
    """
    Convert speed layer window results into recent workload by kitchen station.

    The speed layer stores the top kitchen station per window.
    Here we aggregate those recent top-station counts.
    """
    print("\nPreparing recent speed layer workload...")

    required_columns = [
        "top_kitchen_station",
        "top_station_count",
        "total_value"
    ]

    missing_columns = [col for col in required_columns if col not in speed_df.columns]

    if missing_columns:
        print(f"ERROR: Missing columns in speed layer file: {missing_columns}")
        exit(1)

    speed_summary = speed_df.groupby("top_kitchen_station").agg(
        recent_order_count=("top_station_count", "sum"),
        recent_order_value=("total_value", "sum"),
        windows_as_top_station=("window_number", "count")
    ).reset_index()

    speed_summary.rename(
        columns={"top_kitchen_station": "kitchen_station"},
        inplace=True
    )

    speed_summary["recent_order_count"] = speed_summary["recent_order_count"].astype(int)
    speed_summary["recent_order_value"] = speed_summary["recent_order_value"].round(2)

    return speed_summary


def classify_workload(recent_count, max_recent_count):
    """
    Assign a simple workload status.

    Logic:
    - If there is no recent activity, mark as Normal.
    - If the station has 60% or more of the maximum recent count, mark Overloaded.
    - If the station has 30% or more of the maximum recent count, mark Busy.
    - Otherwise mark Normal.
    """
    if max_recent_count == 0 or recent_count == 0:
        return "Normal"

    recent_ratio = recent_count / max_recent_count

    if recent_ratio >= 0.60:
        return "Overloaded"
    elif recent_ratio >= 0.30:
        return "Busy"
    else:
        return "Normal"


def create_serving_view(batch_df, speed_summary):
    """
    Merge historical batch workload with recent speed workload.
    """
    print("Creating serving view by merging batch and speed results...")

    required_batch_columns = [
        "kitchen_station",
        "order_count",
        "total_quantity",
        "total_value"
    ]

    missing_columns = [col for col in required_batch_columns if col not in batch_df.columns]

    if missing_columns:
        print(f"ERROR: Missing columns in batch workload file: {missing_columns}")
        exit(1)

    # Rename batch columns for clarity
    batch_clean = batch_df[
        [
            "kitchen_station",
            "order_count",
            "total_quantity",
            "total_value"
        ]
    ].copy()

    batch_clean.rename(
        columns={
            "order_count": "historical_order_count",
            "total_quantity": "historical_quantity",
            "total_value": "historical_order_value"
        },
        inplace=True
    )

    # Merge batch and recent speed results
    serving_df = pd.merge(
        batch_clean,
        speed_summary,
        on="kitchen_station",
        how="left"
    )

    # Fill stations with no recent activity
    serving_df["recent_order_count"] = serving_df["recent_order_count"].fillna(0).astype(int)
    serving_df["recent_order_value"] = serving_df["recent_order_value"].fillna(0.0).round(2)
    serving_df["windows_as_top_station"] = serving_df["windows_as_top_station"].fillna(0).astype(int)

    # Calculate recent percentage
    total_recent_orders = serving_df["recent_order_count"].sum()

    if total_recent_orders > 0:
        serving_df["recent_workload_percentage"] = (
            serving_df["recent_order_count"] / total_recent_orders * 100
        ).round(2)
    else:
        serving_df["recent_workload_percentage"] = 0.0

    # Calculate workload status
    max_recent_count = serving_df["recent_order_count"].max()

    serving_df["workload_status"] = serving_df["recent_order_count"].apply(
        lambda count: classify_workload(count, max_recent_count)
    )

    # Sort highest recent workload first
    serving_df = serving_df.sort_values(
        by=["recent_order_count", "historical_order_count"],
        ascending=False
    )

    return serving_df


def main():
    print("=" * 70)
    print("Serving Layer: Combining Batch Layer and Speed Layer Results")
    print("=" * 70)

    # Load input files
    batch_df = load_file(BATCH_WORKLOAD_FILE, "Batch kitchen workload")
    speed_df = load_file(SPEED_RESULTS_FILE, "Speed layer results")

    # Prepare speed layer summary
    speed_summary = prepare_speed_summary(speed_df)

    # Create final serving view
    serving_df = create_serving_view(batch_df, speed_summary)

    # Save output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    serving_df.to_csv(OUTPUT_FILE, index=False)

    # Print results
    print("\n" + "=" * 70)
    print("Final Serving View")
    print("=" * 70)
    print(serving_df.to_string(index=False))

    print("\nWorkload Status Summary:")
    status_summary = serving_df["workload_status"].value_counts()

    for status, count in status_summary.items():
        print(f"  {status}: {count} kitchen station(s)")

    print("\nServing layer completed successfully.")
    print(f"Output saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()