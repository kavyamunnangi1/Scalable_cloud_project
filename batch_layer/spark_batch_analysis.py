"""
spark_batch_analysis.py

The Batch Layer — analyses the full historical dataset of restaurant orders.
This is the "slow but thorough" side of the Lambda Architecture.

It tries to use PySpark for big-data style processing.
If PySpark is not available (e.g. on Replit), it falls back to pandas.

Outputs saved to results/:
    batch_orders_by_restaurant.csv
    batch_orders_by_hour.csv
    batch_popular_items.csv
    batch_revenue_by_restaurant.csv
    batch_kitchen_workload.csv

Usage:
    python batch_layer/spark_batch_analysis.py
"""

import os
import pandas as pd

# -------------------------------------------------------
# File paths
# -------------------------------------------------------
INPUT_FILE = "data/processed/combined_orders.csv"
RESULTS_DIR = "results"


def load_data_pandas(filepath):
    """Load the combined orders CSV using pandas."""
    if not os.path.exists(filepath):
        print(f"ERROR: File not found: {filepath}")
        print("  -> Run 'python data/prepare_dataset.py' first.")
        exit(1)
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} rows with pandas.")
    return df


def run_with_pyspark(filepath):
    """
    Attempt to run the batch analysis using PySpark.
    Returns True if successful, False if PySpark is not available.
    """
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F

        print("PySpark found! Running batch analysis with Spark...")

        # Create a local Spark session (no cluster needed for Replit testing)
        spark = SparkSession.builder \
            .appName("RestaurantBatchAnalysis") \
            .master("local[*]") \
            .config("spark.driver.memory", "512m") \
            .getOrCreate()

        # Reduce Spark log noise
        spark.sparkContext.setLogLevel("ERROR")

        # Load the CSV
        df = spark.read.csv(filepath, header=True, inferSchema=True)
        total_rows = df.count()
        print(f"  Total rows in dataset: {total_rows}")

        # ---- 1. Orders by restaurant ----
        orders_by_restaurant = df.groupBy("restaurant_id") \
            .agg(F.count("*").alias("order_count")) \
            .orderBy("order_count", ascending=False)

        # ---- 2. Orders by hour ----
        orders_by_hour = df.groupBy("order_hour") \
            .agg(F.count("*").alias("order_count")) \
            .orderBy("order_hour")

        # ---- 3. Top 10 popular items ----
        popular_items = df.groupBy("item_name") \
            .agg(F.sum("quantity").alias("total_quantity")) \
            .orderBy("total_quantity", ascending=False) \
            .limit(10)

        # ---- 4. Revenue by restaurant ----
        revenue_by_restaurant = df.groupBy("restaurant_id") \
            .agg(F.round(F.sum("order_item_value"), 2).alias("total_revenue")) \
            .orderBy("total_revenue", ascending=False)

        # ---- 5. Kitchen station workload ----
        kitchen_workload = df.groupBy("kitchen_station") \
            .agg(
                F.count("*").alias("order_count"),
                F.sum("quantity").alias("total_quantity"),
                F.round(F.sum("order_item_value"), 2).alias("total_value")
            ) \
            .orderBy("order_count", ascending=False)

        # ---- Save outputs ----
        os.makedirs(RESULTS_DIR, exist_ok=True)

        def save_spark_df(spark_df, filename):
            """Convert Spark DataFrame to pandas and save as CSV."""
            pandas_df = spark_df.toPandas()
            path = os.path.join(RESULTS_DIR, filename)
            pandas_df.to_csv(path, index=False)
            print(f"  Saved: {path}")

        save_spark_df(orders_by_restaurant, "batch_orders_by_restaurant.csv")
        save_spark_df(orders_by_hour,       "batch_orders_by_hour.csv")
        save_spark_df(popular_items,        "batch_popular_items.csv")
        save_spark_df(revenue_by_restaurant,"batch_revenue_by_restaurant.csv")
        save_spark_df(kitchen_workload,     "batch_kitchen_workload.csv")

        spark.stop()
        return True

    except ImportError:
        print("PySpark not available — falling back to pandas.")
        return False
    except Exception as e:
        print(f"PySpark failed with error: {e}")
        print("Falling back to pandas...")
        return False


def run_with_pandas(filepath):
    """
    Run the full batch analysis using pandas.
    This is the fallback when PySpark is not available.
    """
    print("Running batch analysis with pandas...")
    df = load_data_pandas(filepath)

    os.makedirs(RESULTS_DIR, exist_ok=True)

    # ---- Summary stats ----
    total_rows   = len(df)
    unique_orders = df["order_id"].nunique()
    print(f"\n  Total rows in dataset : {total_rows}")
    print(f"  Unique orders         : {unique_orders}")

    # ---- 1. Orders by restaurant ----
    orders_by_restaurant = df.groupby("restaurant_id").size().reset_index(name="order_count")
    orders_by_restaurant = orders_by_restaurant.sort_values("order_count", ascending=False)
    path = os.path.join(RESULTS_DIR, "batch_orders_by_restaurant.csv")
    orders_by_restaurant.to_csv(path, index=False)
    print(f"  Saved: {path}")

    # ---- 2. Orders by hour ----
    orders_by_hour = df.groupby("order_hour").size().reset_index(name="order_count")
    orders_by_hour = orders_by_hour.sort_values("order_hour")
    path = os.path.join(RESULTS_DIR, "batch_orders_by_hour.csv")
    orders_by_hour.to_csv(path, index=False)
    print(f"  Saved: {path}")

    # ---- 3. Top 10 popular items ----
    popular_items = df.groupby("item_name")["quantity"].sum().reset_index(name="total_quantity")
    popular_items = popular_items.sort_values("total_quantity", ascending=False).head(10)
    path = os.path.join(RESULTS_DIR, "batch_popular_items.csv")
    popular_items.to_csv(path, index=False)
    print(f"  Saved: {path}")

    # ---- 4. Revenue by restaurant ----
    revenue_by_restaurant = df.groupby("restaurant_id")["order_item_value"].sum().reset_index(name="total_revenue")
    revenue_by_restaurant["total_revenue"] = revenue_by_restaurant["total_revenue"].round(2)
    revenue_by_restaurant = revenue_by_restaurant.sort_values("total_revenue", ascending=False)
    path = os.path.join(RESULTS_DIR, "batch_revenue_by_restaurant.csv")
    revenue_by_restaurant.to_csv(path, index=False)
    print(f"  Saved: {path}")

    # ---- 5. Kitchen station workload ----
    kitchen_workload = df.groupby("kitchen_station").agg(
        order_count   = ("event_id",         "count"),
        total_quantity= ("quantity",          "sum"),
        total_value   = ("order_item_value",  "sum"),
    ).reset_index()
    kitchen_workload["total_value"] = kitchen_workload["total_value"].round(2)
    kitchen_workload = kitchen_workload.sort_values("order_count", ascending=False)
    path = os.path.join(RESULTS_DIR, "batch_kitchen_workload.csv")
    kitchen_workload.to_csv(path, index=False)
    print(f"  Saved: {path}")

    # ---- Print batch summary ----
    print("\nBatch Analysis Summary:")
    print(f"  Total rows         : {total_rows}")
    print(f"  Unique orders      : {unique_orders}")
    print("\n  Orders by restaurant:")
    print(orders_by_restaurant.to_string(index=False))
    print("\n  Revenue by restaurant:")
    print(revenue_by_restaurant.to_string(index=False))
    print("\n  Top 10 popular items:")
    print(popular_items.head(10).to_string(index=False))
    print("\n  Kitchen station workload:")
    print(kitchen_workload.to_string(index=False))


def main():
    print("=" * 60)
    print("  Batch Layer — Full Historical Analysis")
    print("=" * 60)

    # Try PySpark first; fall back to pandas if it fails
    spark_success = run_with_pyspark(INPUT_FILE)
    if not spark_success:
        run_with_pandas(INPUT_FILE)

    print("\nBatch layer complete! Results saved to:", RESULTS_DIR)


if __name__ == "__main__":
    main()
