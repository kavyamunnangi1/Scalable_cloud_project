"""
prepare_dataset.py

This script reads the two raw restaurant order CSV files,
cleans and combines them, and saves the result to:
    data/processed/combined_orders.csv

Run this script first before any other pipeline step.

Usage:
    python data/prepare_dataset.py
"""

import os
import pandas as pd


# -------------------------------------------------------
# File paths
# -------------------------------------------------------
RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

R1_ORDERS = os.path.join(RAW_DIR, "restaurant-1-orders.csv")
R2_ORDERS = os.path.join(RAW_DIR, "restaurant-2-orders.csv")
R1_PRICES = os.path.join(RAW_DIR, "restaurant-1-products-price.csv")
R2_PRICES = os.path.join(RAW_DIR, "restaurant-2-products-price.csv")

OUTPUT_FILE = os.path.join(PROCESSED_DIR, "combined_orders.csv")


def load_csv(filepath):
    """
    Load a CSV file and return a DataFrame.
    """
    if not os.path.exists(filepath):
        print(f"ERROR: File not found: {filepath}")
        exit(1)

    df = pd.read_csv(filepath, encoding="utf-8-sig")
    print(f"  Loaded {len(df)} rows from {filepath}")
    return df


def assign_kitchen_station(item_name):
    """
    Map an item name to a kitchen station.
    This is a simple rule-based method for kitchen workload analysis.
    """
    item_lower = str(item_name).lower()

    if any(word in item_lower for word in ["naan", "roti", "paratha", "chapati", "puree", "kulcha"]):
        return "Bread Station"

    elif any(word in item_lower for word in ["rice", "pilau", "biryani", "fried rice"]):
        return "Rice Station"

    elif any(word in item_lower for word in [
        "curry", "masala", "korma", "balti", "madras", "vindaloo",
        "bhuna", "jalfrezi", "dupiaza", "dansak", "pathia", "saag"
    ]):
        return "Hot Kitchen"

    elif any(word in item_lower for word in ["tikka", "kebab", "tandoori", "mixed grill", "grill"]):
        return "Grill Station"

    elif any(word in item_lower for word in ["samosa", "pakora", "chips", "fried", "puri"]):
        return "Fryer"

    elif any(word in item_lower for word in ["coke", "lemonade", "water", "cobra", "beer", "bottle", "diet", "drink"]):
        return "Drinks"

    elif any(word in item_lower for word in [
        "sauce", "chutney", "pickle", "raitha", "raita",
        "salad", "papadum", "papadom", "mint"
    ]):
        return "Condiments"

    else:
        return "General Kitchen"


def prepare_price_lookup(prices_df):
    """
    Prepare product price lookup file.

    This is used only as supporting data. If the order file already has price,
    that price is kept. Lookup price is used only when product price is missing
    or zero.
    """
    df = prices_df.copy()

    rename_map = {}
    for col in df.columns:
        col_lower = col.strip().lower()

        if "item name" in col_lower:
            rename_map[col] = "item_name"
        elif "product price" in col_lower or "price" in col_lower:
            rename_map[col] = "lookup_product_price"

    df.rename(columns=rename_map, inplace=True)

    if "item_name" not in df.columns or "lookup_product_price" not in df.columns:
        return pd.DataFrame(columns=["item_name", "lookup_product_price"])

    df["lookup_product_price"] = pd.to_numeric(
        df["lookup_product_price"],
        errors="coerce"
    ).fillna(0.0)

    df = df[["item_name", "lookup_product_price"]].drop_duplicates()

    return df


def prepare_restaurant(orders_df, prices_df, restaurant_id):
    """
    Clean and enrich a single restaurant's orders DataFrame.
    """
    df = orders_df.copy()

    # ---------------------------------------------------
    # Rename columns to a common standard
    # ---------------------------------------------------
    rename_map = {}

    for col in df.columns:
        col_lower = col.strip().lower()

        if "order number" in col_lower or "order id" in col_lower:
            rename_map[col] = "original_order_id"
        elif "order date" in col_lower:
            rename_map[col] = "order_date_raw"
        elif "item name" in col_lower:
            rename_map[col] = "item_name"
        elif "quantity" in col_lower:
            rename_map[col] = "quantity"
        elif "product price" in col_lower:
            rename_map[col] = "product_price"
        elif "total products" in col_lower:
            rename_map[col] = "total_products"

    df.rename(columns=rename_map, inplace=True)

    required_columns = [
        "original_order_id",
        "order_date_raw",
        "item_name",
        "quantity",
        "product_price",
        "total_products"
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        print(f"ERROR: Missing columns for {restaurant_id}: {missing_columns}")
        exit(1)

    # ---------------------------------------------------
    # Parse date
    # ---------------------------------------------------
    df["order_date"] = pd.to_datetime(
        df["order_date_raw"],
        dayfirst=True,
        errors="coerce"
    )

    bad_dates = df["order_date"].isna().sum()

    if bad_dates > 0:
        print(f"  WARNING: Dropping {bad_dates} rows with invalid dates in {restaurant_id}")

    df = df.dropna(subset=["order_date"])

    # ---------------------------------------------------
    # Clean numeric fields
    # ---------------------------------------------------
    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    ).fillna(0).astype(int)

    df["product_price"] = pd.to_numeric(
        df["product_price"],
        errors="coerce"
    ).fillna(0.0)

    df["total_products"] = pd.to_numeric(
        df["total_products"],
        errors="coerce"
    ).fillna(0).astype(int)

    # ---------------------------------------------------
    # Use product price lookup to fill missing/zero prices
    # ---------------------------------------------------
    price_lookup = prepare_price_lookup(prices_df)

    if not price_lookup.empty:
        df = df.merge(price_lookup, on="item_name", how="left")

        df["lookup_product_price"] = df["lookup_product_price"].fillna(0.0)

        df["product_price"] = df.apply(
            lambda row: row["lookup_product_price"]
            if row["product_price"] == 0 and row["lookup_product_price"] > 0
            else row["product_price"],
            axis=1
        )

        df.drop(columns=["lookup_product_price"], inplace=True)

    # ---------------------------------------------------
    # Create derived fields
    # ---------------------------------------------------
    df["restaurant_id"] = restaurant_id

    df["order_id"] = (
        df["restaurant_id"] + "_" + df["original_order_id"].astype(str)
    )

    df["event_time"] = df["order_date"]
    df["order_date_str"] = df["order_date"].dt.strftime("%Y-%m-%d")
    df["order_hour"] = df["order_date"].dt.hour
    df["day_name"] = df["order_date"].dt.day_name()

    df["order_item_value"] = df["quantity"] * df["product_price"]

    df["kitchen_station"] = df["item_name"].apply(assign_kitchen_station)

    # ---------------------------------------------------
    # Keep final useful columns
    # ---------------------------------------------------
    df = df[
        [
            "restaurant_id",
            "order_id",
            "original_order_id",
            "event_time",
            "order_date_str",
            "order_hour",
            "day_name",
            "item_name",
            "quantity",
            "product_price",
            "total_products",
            "order_item_value",
            "kitchen_station",
        ]
    ]

    df.rename(columns={"order_date_str": "order_date"}, inplace=True)

    return df


def main():
    print("=" * 60)
    print("Dataset Preparation Script")
    print("=" * 60)

    os.makedirs(PROCESSED_DIR, exist_ok=True)

    # ---------------------------------------------------
    # Load raw files
    # ---------------------------------------------------
    print("\nLoading raw order files...")
    r1_orders = load_csv(R1_ORDERS)
    r2_orders = load_csv(R2_ORDERS)

    print("\nLoading product price files...")
    r1_prices = load_csv(R1_PRICES)
    r2_prices = load_csv(R2_PRICES)

    # ---------------------------------------------------
    # Prepare each restaurant
    # ---------------------------------------------------
    print("\nProcessing Restaurant 1...")
    r1_clean = prepare_restaurant(r1_orders, r1_prices, "restaurant_1")
    print(f"  Restaurant 1 processed rows: {len(r1_clean)}")

    print("\nProcessing Restaurant 2...")
    r2_clean = prepare_restaurant(r2_orders, r2_prices, "restaurant_2")
    print(f"  Restaurant 2 processed rows: {len(r2_clean)}")

    # ---------------------------------------------------
    # Combine datasets
    # ---------------------------------------------------
    print("\nCombining restaurant datasets...")

    combined = pd.concat([r1_clean, r2_clean], ignore_index=True)

    combined.sort_values("event_time", inplace=True)
    combined.reset_index(drop=True, inplace=True)

    # Add global event id for simulated streaming
    combined.insert(0, "event_id", range(1, len(combined) + 1))

    # ---------------------------------------------------
    # Save final combined dataset
    # ---------------------------------------------------
    combined.to_csv(OUTPUT_FILE, index=False)

    print("\nDataset preparation completed successfully.")
    print(f"Output file: {OUTPUT_FILE}")
    print(f"Total item-level rows: {len(combined)}")
    print(f"Total unique orders: {combined['order_id'].nunique()}")
    print(f"Date range: {combined['event_time'].min()} to {combined['event_time'].max()}")
    print(f"Unique food items: {combined['item_name'].nunique()}")

    print("\nRows by restaurant:")
    print(combined["restaurant_id"].value_counts())

    print("\nKitchen station summary:")
    print(combined["kitchen_station"].value_counts())

    print("\nSample rows:")
    print(combined.head(3).to_string(index=False))

    print("\nDone. Now you can run the stream producer, speed layer and batch layer.")


if __name__ == "__main__":
    main()