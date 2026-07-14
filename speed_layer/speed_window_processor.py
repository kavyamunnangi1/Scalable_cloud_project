"""
speed_window_processor.py

The Speed Layer — processes a recent window of restaurant order events
to give near-real-time insights. This is the "fast" side of Lambda Architecture.

It reads the most recent --limit records and splits them into --window sized
mini-batches, printing a summary for each window.

Final results are saved to:
    results/speed_layer_results.csv

Usage:
    python speed_layer/speed_window_processor.py
    python speed_layer/speed_window_processor.py --input data/processed/combined_orders.csv --limit 500 --window 50
"""

import argparse
import os
import pandas as pd

# -------------------------------------------------------
# File paths
# -------------------------------------------------------
DEFAULT_INPUT = "data/processed/combined_orders.csv"
OUTPUT_FILE   = "results/speed_layer_results.csv"

# If recent order count in a window exceeds this, flag it as overloaded
OVERLOAD_THRESHOLD = 40


def load_recent_records(filepath, limit):
    """Load only the most recent 'limit' rows from the dataset."""
    if not os.path.exists(filepath):
        print(f"ERROR: File not found: {filepath}")
        print("  -> Run 'python data/prepare_dataset.py' first.")
        exit(1)
    df = pd.read_csv(filepath)
    # Take the last 'limit' rows (most recent events)
    df = df.tail(limit).reset_index(drop=True)
    print(f"Loaded {len(df)} recent records from {filepath}")
    return df


def process_window(window_df, window_number):
    """
    Process a single time window and return a summary dictionary.

    Each window represents a mini-batch of recent orders — like a
    5-second or 1-minute sliding window in a real stream.
    """
    if window_df.empty:
        return None

    # Total number of order line items in this window
    order_count = len(window_df)

    # Total quantity of all items ordered
    total_quantity = int(window_df["quantity"].sum())

    # Total revenue value in this window
    total_value = round(float(window_df["order_item_value"].sum()), 2)

    # Which kitchen station handled the most items?
    station_counts = window_df["kitchen_station"].value_counts()
    top_station = station_counts.idxmax() if not station_counts.empty else "Unknown"
    top_station_count = int(station_counts.max()) if not station_counts.empty else 0

    # Which food item was ordered most often?
    item_counts = window_df["item_name"].value_counts()
    top_item = item_counts.idxmax() if not item_counts.empty else "Unknown"
    top_item_count = int(item_counts.max()) if not item_counts.empty else 0

    # Overload alert — flag if this window has too many orders for one station
    overload_alert = "YES" if top_station_count >= OVERLOAD_THRESHOLD else "NO"

    return {
        "window_number":     window_number,
        "order_count":       order_count,
        "total_quantity":    total_quantity,
        "total_value":       total_value,
        "top_kitchen_station": top_station,
        "top_station_count": top_station_count,
        "top_food_item":     top_item,
        "top_item_count":    top_item_count,
        "overload_alert":    overload_alert,
    }


def main():
    # ---- Parse command-line arguments ----
    parser = argparse.ArgumentParser(description="Speed Layer: Window-based Order Processor")
    parser.add_argument("--input",  type=str, default=DEFAULT_INPUT, help="Input CSV file")
    parser.add_argument("--limit",  type=int, default=500,           help="Number of recent records to process (default: 500)")
    parser.add_argument("--window", type=int, default=50,            help="Window size in records (default: 50)")
    args = parser.parse_args()

    print("=" * 60)
    print("  Speed Layer — Window-based Stream Processor")
    print("=" * 60)

    # ---- Load recent records ----
    df = load_recent_records(args.input, args.limit)

    # ---- Slide through windows ----
    results = []
    num_windows = len(df) // args.window
    remainder   = len(df) % args.window

    print(f"\nProcessing {len(df)} records in windows of {args.window}...")
    print(f"  -> {num_windows} full windows + {remainder} leftover records\n")

    for i in range(num_windows):
        start = i * args.window
        end   = start + args.window
        window_df = df.iloc[start:end]
        summary = process_window(window_df, window_number=i + 1)
        if summary:
            results.append(summary)
            alert_flag = " *** OVERLOAD ALERT ***" if summary["overload_alert"] == "YES" else ""
            print(
                f"Window {summary['window_number']:03d} | "
                f"Orders: {summary['order_count']:4d} | "
                f"Value: £{summary['total_value']:8.2f} | "
                f"Top Station: {summary['top_kitchen_station']:<20s} | "
                f"Top Item: {summary['top_food_item']}{alert_flag}"
            )

    # ---- Process leftover records as a partial last window ----
    if remainder > 0:
        window_df = df.iloc[num_windows * args.window:]
        summary = process_window(window_df, window_number=num_windows + 1)
        if summary:
            results.append(summary)
            print(
                f"Window {summary['window_number']:03d} | "
                f"Orders: {summary['order_count']:4d} | "
                f"Value: £{summary['total_value']:8.2f} | "
                f"Top Station: {summary['top_kitchen_station']:<20s} | "
                f"Top Item: {summary['top_food_item']} (partial window)"
            )

    # ---- Save results ----
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    results_df = pd.DataFrame(results)
    results_df.to_csv(OUTPUT_FILE, index=False)

    # ---- Print overall summary ----
    print("\n" + "-" * 60)
    print("Speed Layer Summary:")
    print(f"  Total windows processed : {len(results)}")
    print(f"  Total records processed : {len(df)}")
    print(f"  Total order value       : £{results_df['total_value'].sum():.2f}")
    print(f"  Overload alerts raised  : {(results_df['overload_alert'] == 'YES').sum()}")
    print(f"  Results saved to        : {OUTPUT_FILE}")
    print("\nDone!")


if __name__ == "__main__":
    main()
