# Architecture Notes

## Project Architecture

This project uses a Lambda Architecture for scalable restaurant order analytics. The system takes restaurant order data, simulates it as a continuous stream, sends it to AWS Kinesis and stores the results in Amazon S3.

The architecture has three main processing layers:

```text
1. Batch Layer
2. Speed Layer
3. Serving Layer
```

The batch layer is used for complete historical analysis.  
The speed layer is used for recent window based analysis.  
The serving layer combines both outputs into one final result.

## Architecture Diagram

```text
+----------------------------+
|  Restaurant Order Dataset  |
|  CSV files                 |
+-------------+--------------+
              |
              v
+----------------------------+
|  Data Preparation Script   |
|  prepare_dataset.py        |
+-------------+--------------+
              |
              v
+----------------------------+
|  Combined Orders Dataset   |
|  combined_orders.csv       |
+-------------+--------------+
              |
              v
+----------------------------+
|  Stream Producer           |
|  stream_producer.py        |
|  Simulates live orders     |
+-------------+--------------+
              |
              v
+----------------------------+
|  AWS Kinesis Data Streams  |
|  restaurant-order-stream   |
+-------------+--------------+
              |
              v
+----------------------------+
|  Kinesis to S3 Consumer    |
|  kinesis_to_s3_consumer.py |
+-------------+--------------+
              |
              v
+----------------------------+
|  Amazon S3 raw/            |
|  Stores streamed records   |
+-------------+--------------+
              |
              v
+--------------------------------------------------+
|              Lambda Architecture                 |
|                                                  |
|  +--------------------+   +--------------------+ |
|  | Batch Layer         |   | Speed Layer         | |
|  | Full history        |   | Recent windows      | |
|  | PySpark / pandas    |   | Window processing   | |
|  +----------+---------+   +----------+---------+ |
|             |                        |           |
|             v                        v           |
|  +--------------------+   +--------------------+ |
|  | Batch Results       |   | Speed Results       | |
|  | S3 batch-results/   |   | S3 speed-results/   | |
|  +----------+---------+   +----------+---------+ |
|             |                        |           |
|             +-----------+------------+           |
|                         |                        |
|                         v                        |
|              +--------------------+              |
|              | Serving Layer       |              |
|              | combine_results.py  |              |
|              +----------+---------+              |
|                         |                        |
|                         v                        |
|              +--------------------+              |
|              | Final Serving View  |              |
|              | S3 serving-results/ |              |
|              +--------------------+              |
+--------------------------------------------------+
```

## AWS Auto Scaling Boundary

The auto scaling part is used to show how compute resources can scale when workload increases.

```text
+--------------------------------------------------+
|        Auto Scaling Infrastructure Boundary       |
|                                                  |
|  +------------------------+                      |
|  | EC2 Launch Template    |                      |
|  | restaurant-worker-     |                      |
|  | template               |                      |
|  +-----------+------------+                      |
|              |                                   |
|              v                                   |
|  +------------------------+                      |
|  | Auto Scaling Group     |                      |
|  | restaurant-worker-asg  |                      |
|  +-----------+------------+                      |
|              |                                   |
|              v                                   |
|  +------------------------+                      |
|  | EC2 Worker Instance    |                      |
|  | t2.micro               |                      |
|  +------------------------+                      |
|                                                  |
|  Min capacity: 1                                 |
|  Desired capacity: 1                             |
|  Max capacity: 2                                 |
|  Scaling trigger: Average CPU utilisation 30%    |
|  Instance warmup: 120 seconds                    |
+--------------------------------------------------+
```

## Data Flow Explanation

### Step 1: Dataset Preparation

The project starts with four raw CSV files:

```text
restaurant-1-orders.csv
restaurant-1-products-price.csv
restaurant-2-orders.csv
restaurant-2-products-price.csv
```

The `prepare_dataset.py` script cleans and combines these files into:

```text
data/processed/combined_orders.csv
```

This file is used by all other parts of the project.

### Step 2: Stream Simulation

The `stream_producer.py` script reads rows from the processed dataset and sends them one by one.

It can run in two modes:

```text
Local mode: prints events in the terminal
AWS mode: sends events to AWS Kinesis
```

Example AWS command:

```bash
python producer/stream_producer.py --limit 20 --rate 2 --aws
```

### Step 3: Ingestion Layer

AWS Kinesis Data Streams is used as the ingestion service.

```text
Kinesis stream name: restaurant-order-stream
```

The stream receives restaurant order events from the Python producer.

### Step 4: Raw Storage

The `kinesis_to_s3_consumer.py` script reads records from Kinesis and uploads them to Amazon S3.

S3 raw output location:

```text
s3://restaurant-order-analytics/raw/
```

This keeps a copy of incoming streamed events.

### Step 5: Batch Layer

The batch layer analyses the full historical dataset.

Script used:

```text
batch_layer/spark_batch_analysis.py
```

Main outputs:

```text
batch_orders_by_restaurant.csv
batch_orders_by_hour.csv
batch_popular_items.csv
batch_revenue_by_restaurant.csv
batch_kitchen_workload.csv
```

These outputs are uploaded to:

```text
s3://restaurant-order-analytics/batch-results/
```

### Step 6: Speed Layer

The speed layer processes recent records in small windows.

Script used:

```text
speed_layer/speed_window_processor.py
```

Example:

```bash
python speed_layer/speed_window_processor.py --input data/processed/combined_orders.csv --limit 500 --window 50
```

This processes 500 recent records in 10 windows of 50 records.

Output:

```text
speed_layer_results.csv
```

S3 location:

```text
s3://restaurant-order-analytics/speed-results/
```

### Step 7: Serving Layer

The serving layer combines the batch layer output and speed layer output.

Script used:

```text
serving_layer/combine_results.py
```

Output:

```text
serving_view.csv
```

S3 location:

```text
s3://restaurant-order-analytics/serving-results/
```

The serving view shows the final workload status for each kitchen station.

## Why This Architecture Was Used

This architecture was selected because restaurant order data needs both historical and recent analysis.

A batch only system would show accurate historical results, but it would not show recent workload quickly.

A stream only system would show recent results, but it would not give complete historical summaries.

Lambda Architecture is useful because it provides both:

```text
Batch layer = accuracy over full history
Speed layer = freshness over recent data
Serving layer = combined final result
```

## S3 Folder Design

The S3 bucket is organised by pipeline stage:

```text
restaurant-order-analytics/
│
├── raw/
│   └── Kinesis consumed records
│
├── processed/
│   └── combined_orders.csv
│
├── batch-results/
│   └── historical analytics outputs
│
├── speed-results/
│   └── recent window analytics outputs
│
├── serving-results/
│   └── final merged serving view
│
└── performance/
    └── benchmark metrics and graphs
```

This makes it easier to explain the flow of data in the report and demo.

## Auto Scaling Design

EC2 Auto Scaling was added to show elastic compute infrastructure.

Configuration:

```text
Launch template: restaurant-worker-template
Auto Scaling group: restaurant-worker-asg
Instance type: t2.micro
Minimum capacity: 1
Desired capacity: 1
Maximum capacity: 2
Scaling policy: Target tracking
Metric: Average CPU utilisation
Target value: 30%
Instance warmup: 120 seconds
```

The Auto Scaling Group successfully launched a healthy EC2 instance. CloudWatch monitoring was enabled to view EC2 and group metrics.

## Performance Measurement

The performance benchmark script tests different stream rates, record limits and worker counts.

Script used:

```text
performance/benchmark_runner.py
```

Outputs:

```text
performance_metrics.csv
latency_vs_rate.png
throughput_vs_rate.png
processing_time_vs_records.png
speedup_vs_worker_count.png
```

These outputs are stored locally and uploaded to:

```text
s3://restaurant-order-analytics/performance/
```

## Main Project Outputs

```text
Processed dataset:
data/processed/combined_orders.csv

Batch layer:
results/batch_kitchen_workload.csv
results/batch_orders_by_hour.csv
results/batch_popular_items.csv
results/batch_revenue_by_restaurant.csv
results/batch_orders_by_restaurant.csv

Speed layer:
results/speed_layer_results.csv

Serving layer:
results/serving_view.csv

Performance:
results/performance_metrics.csv
results/*.png
```

The architecture successfully demonstrates a Lambda Architecture pipeline for restaurant order analytics. It includes stream ingestion using AWS Kinesis, storage using Amazon S3, batch processing, speed layer window processing, serving layer result merging and EC2 Auto Scaling for elastic compute demonstration.