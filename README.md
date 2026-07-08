# Scalable Cloud Based Restaurant Order Analytics

## Project Overview

This project is a scalable cloud based real time analytics system for restaurant takeaway orders. The main idea is to replay a historical restaurant order dataset as a live stream and process it using a Lambda Architecture.

The system has three main layers:

- Batch layer
- Speed layer
- Serving layer

The batch layer analyses the full historical dataset. The speed layer processes recent records in small windows. The serving layer combines both outputs and creates a final kitchen workload view.

AWS is used for cloud ingestion, cloud storage and auto scaling demonstration.

## Problem Statement

Restaurants receive many takeaway orders during busy hours. Some kitchen stations may become overloaded while others may still have normal workload. This project helps to identify which kitchen stations are busy by analysing restaurant order data.

The main question answered by this project is:

**How can a cloud based Lambda Architecture be used to monitor restaurant order queues and kitchen workload in near real time?**

## Dataset

The dataset used in this project is a public takeaway food orders dataset. It contains order and product price files for two restaurants. (https://www.kaggle.com/datasets/henslersoftware/19560-indian-takeaway-orders)

The raw CSV files used are:

```text
restaurant-1-orders.csv
restaurant-1-products-price.csv
restaurant-2-orders.csv
restaurant-2-products-price.csv
```

These files are cleaned and combined into one processed file:

```text
data/processed/combined_orders.csv
```

After preparation, the combined dataset contains around 194,001 order item rows.

The prepared dataset contains columns such as:

```text
event_id
restaurant_id
order_id
event_time
order_date
order_hour
day_name
item_name
quantity
product_price
total_products
order_item_value
kitchen_station
```

Kitchen stations were added using simple rule based mapping from item names. Examples of kitchen stations are:

```text
Hot Kitchen
General Kitchen
Rice Station
Bread Station
Grill Station
Condiments
Fryer
Drinks
```

## Lambda Architecture

This project uses Lambda Architecture because it needs both full historical analysis and recent stream analysis.

The batch layer gives accurate results from all available data.  
The speed layer gives recent low latency results.  
The serving layer combines the batch and speed outputs into one final view.

Basic architecture flow:

```text
Raw Restaurant Dataset
        |
        v
Data Preparation
        |
        v
Stream Producer
        |
        v
AWS Kinesis Data Streams
        |
        v
Kinesis Consumer
        |
        v
Amazon S3 Raw Storage
        |
        +----------------------+
        |                      |
        v                      v
Batch Layer              Speed Layer
Full History             Recent Windows
        |                      |
        v                      v
Batch Results            Speed Results
        |                      |
        +----------+-----------+
                   |
                   v
             Serving Layer
                   |
                   v
            Final Serving View
```

## AWS Services Used

The following AWS services are used in this project:

```text
Amazon Kinesis Data Streams
Amazon S3
Amazon EC2
EC2 Launch Template
EC2 Auto Scaling Group
Amazon CloudWatch
```

AWS resource names used:

```text
Kinesis stream: restaurant-order-stream
S3 bucket: restaurant-order-analytics
Launch template: restaurant-worker-template
Auto Scaling group: restaurant-worker-asg
Scaling policy: cpu-target-tracking-policy
```

## Project Structure

```text
Cloud_Scalable/
│
├── batch_layer/
│   └── spark_batch_analysis.py
│
├── data/
│   ├── raw/
│   ├── processed/
│   ├── prepare_dataset.py
│   └── README.md
│
├── diagrams/
│   └── architecture_notes.md
│
├── performance/
│   └── benchmark_runner.py
│
├── producer/
│   ├── stream_producer.py
│   ├── test_aws_connection.py
│   ├── kinesis_to_s3_consumer.py
│   └── upload_outputs_to_s3.py
│
├── results/
│
├── serving_layer/
│   └── combine_results.py
│
├── speed_layer/
│   └── speed_window_processor.py
│
├── .gitignore
├── README.md
└── requirements.txt
```

## Requirements

Install the required Python packages using:

```bash
pip install -r requirements.txt
```

Main libraries used:

```text
pandas
numpy
boto3
pyspark
matplotlib
python-dotenv
```

## Environment File

A `.env` file is used for AWS settings. This file should not be uploaded to GitHub.

Example `.env` format:

```env
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_SESSION_TOKEN=your_session_token
AWS_REGION=us-east-1
KINESIS_STREAM_NAME=restaurant-order-stream
S3_BUCKET_NAME=restaurant-order-analytics
```

## How to Run the Project

Run all commands from the main project folder.

### 1. Prepare the Dataset

```bash
python data/prepare_dataset.py
```

This creates:

```text
data/processed/combined_orders.csv
```

### 2. Run the Local Stream Producer

```bash
python producer/stream_producer.py --limit 20 --rate 2
```

This prints restaurant order records one by one like a live stream.

### 3. Save Sample Stream Events Locally

```bash
python producer/stream_producer.py --limit 20 --rate 2 --output results/sample_stream_events.jsonl
```

### 4. Run the Speed Layer

```bash
python speed_layer/speed_window_processor.py --input data/processed/combined_orders.csv --limit 500 --window 50
```

This creates:

```text
results/speed_layer_results.csv
```

### 5. Run the Batch Layer

```bash
python batch_layer/spark_batch_analysis.py
```

This creates batch output files in the `results` folder.

### 6. Run the Serving Layer

```bash
python serving_layer/combine_results.py
```

This creates:

```text
results/serving_view.csv
```

### 7. Run Performance Benchmarking

```bash
python performance/benchmark_runner.py
```

This creates performance metrics and graphs in the `results` folder.

## AWS Run Steps

### 1. Test AWS Connection

```bash
python producer/test_aws_connection.py
```

This checks if the Python code can connect to the Kinesis stream and S3 bucket.

### 2. Send Stream Records to Kinesis

```bash
python producer/stream_producer.py --limit 20 --rate 2 --aws
```

This sends restaurant order events to AWS Kinesis Data Streams.

### 3. Check Records in Kinesis

In AWS Console:

```text
Amazon Kinesis → Data streams → restaurant-order-stream → Data viewer
```

Select:

```text
Shard: shardId-000000000001
Starting position: Trim horizon
```

Then click:

```text
Get records
```

The records should appear in the Kinesis data viewer.

### 4. Read Records from Kinesis and Save to S3

```bash
python producer/kinesis_to_s3_consumer.py --max-records 20
```

This reads records from Kinesis and uploads them to the S3 `raw/` folder.

### 5. Upload Local Outputs to S3

```bash
python producer/upload_outputs_to_s3.py
```

This uploads processed data, batch results, speed results, serving results and performance graphs to S3.

## S3 Folder Structure

The S3 bucket is organised like this:

```text
restaurant-order-analytics/
│
├── raw/
├── processed/
├── batch-results/
├── speed-results/
├── serving-results/
└── performance/
```

## Batch Layer

The batch layer analyses the full historical dataset using PySpark. If PySpark is not available, the script can also use pandas as a fallback.

The batch layer creates these files:

```text
batch_orders_by_restaurant.csv
batch_orders_by_hour.csv
batch_popular_items.csv
batch_revenue_by_restaurant.csv
batch_kitchen_workload.csv
```

These outputs show long term order activity, revenue, popular items and kitchen workload.

## Speed Layer

The speed layer processes recent records using small windows. In this project, 500 recent records are processed in windows of 50 records.

The speed layer calculates:

```text
order count
total quantity
total value
top kitchen station
top food item
overload alert
```

This helps to understand recent kitchen workload.

## Serving Layer

The serving layer combines the batch result and speed result. It creates one final serving view:

```text
results/serving_view.csv
```

The serving view shows historical workload, recent workload and workload status for each kitchen station.

Possible workload statuses are:

```text
Normal
Busy
Overloaded
```

## Performance Testing

The performance script tests different record limits, stream rates and worker counts.

It creates:

```text
performance_metrics.csv
latency_vs_rate.png
throughput_vs_rate.png
processing_time_vs_records.png
speedup_vs_worker_count.png
```

These files are used to discuss throughput, latency and speedup.

## Auto Scaling

EC2 Auto Scaling was used to show elastic compute setup.

Configuration used:

```text
Launch template: restaurant-worker-template
Auto Scaling group: restaurant-worker-asg
Minimum capacity: 1
Desired capacity: 1
Maximum capacity: 2
Scaling metric: Average CPU utilization
Target value: 30%
Instance warmup: 120 seconds
```

The Auto Scaling group successfully launched a healthy EC2 instance. CloudWatch monitoring was also enabled to view group and instance metrics.

## Main Outputs

The main final outputs of the project are:

```text
data/processed/combined_orders.csv
results/speed_layer_results.csv
results/batch_kitchen_workload.csv
results/serving_view.csv
results/performance_metrics.csv
results/*.png
```

## Limitations

The stream is simulated by replaying historical restaurant order data. This is suitable for this project because the project brief allows public datasets to be replayed as a controlled stream.

The local scripts are used for processing and AWS is used for ingestion, storage and auto scaling demonstration. A future version can run the Spark batch layer directly on AWS EMR.

## Future Work

Possible improvements are:

```text
Run PySpark batch processing on AWS EMR
Use AWS Lambda for automatic stream processing
Use Amazon Athena to query S3 files
Create a dashboard connected to S3 results
Add alerts for overloaded kitchen stations
Run larger load tests for stronger auto scaling results
```

This project shows how restaurant order data can be processed using a scalable cloud based Lambda Architecture. The system prepares the dataset, simulates live streaming, sends records to AWS Kinesis, stores records in S3, processes batch and speed results, combines them in a serving layer and demonstrates EC2 Auto Scaling.