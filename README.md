# Scalable Cloud-Based Restaurant Order Queue and Kitchen Load Analytics

## Project Overview
This project develops a scalable cloud-based analytics system for monitoring restaurant order queues and kitchen workload. A public takeaway food orders dataset will be replayed as a continuous stream and ingested into AWS.

The system follows Lambda Architecture, where a batch layer analyses full historical order data and a speed layer processes recent incoming orders using short time windows.

## Selected Dataset
Dataset: Takeaway Food Orders - Updated

Files used:
- restaurant-1-orders.csv
- restaurant-2-orders.csv
- restaurant-1-products-price.csv
- restaurant-2-products-price.csv

The two order files will be combined and replayed as a simulated order stream. The product-price files will be used as supporting lookup files.

## Analytics Question
How can a scalable AWS-based Lambda Architecture monitor restaurant order queues and detect kitchen workload pressure using batch and recent-window analytics?

## Planned Architecture
- Python producer to replay restaurant order records
- AWS Kinesis Data Streams for ingestion
- Amazon S3 for raw and processed data storage
- PySpark / Apache Spark for batch processing
- Speed layer for recent order-window processing
- Serving layer to combine batch and speed results
- Performance testing using latency, throughput and speedup

## Expected Outputs
- Total orders by time period
- Popular food items
- Peak order hours
- Kitchen station workload
- Recent order queue size
- Delay or overload alerts
- Performance graphs

## Technologies
- Python
- AWS Kinesis
- Amazon S3
- PySpark / Apache Spark
- AWS EC2 or EMR
- Athena / Dashboard
- Matplotlib
