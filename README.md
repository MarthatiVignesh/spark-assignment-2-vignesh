# Incremental Order Processing & Sales

## Project Overview
This project uses PySpark Structured Streaming to process daily order CSV files incrementally, validate records, calculate order totals, handle duplicate order IDs, and aggregate cumulative city-wise sales.

## Technologies
- Python 3
- Apache Spark 3.5.3
- PySpark Structured Streaming
- CSV input and Parquet output

## Features
- Incremental ingestion of daily CSV files
- Validation of required fields, quantities, prices, and timestamps
- Calculation of total order value (`quantity × unit_price`)
- UDF-based order value classification
- Duplicate order ID handling
- Cumulative city-wise sales and item totals
- Checkpointing and restart recovery

## Project Structure
- `src/order_streaming.py` — streaming application
- `order_source/` — daily CSV input files
- `output/processed_batches/` — generated batch data
- `output/city_sales/` — cumulative city-wise sales
- `checkpoint/order_stream/` — streaming checkpoint data

## How to Run
From the project root with Python 3 and PySpark 3.5.3 installed:

```bash
python3 src/order_streaming.py
```

Add new daily CSV files under `order_source/` while the stream is running, preserving the same CSV header and columns.

## Restart Recovery
Stop the streaming application and restart it using the same checkpoint directory. Do not delete `checkpoint/order_stream/` if you want Spark to recover saved streaming progress.

## Sample Results
The sample test across three daily files produced:

| City | Total Sales | Items |
|---|---:|---:|
| Delhi | 63,200 | 5 |
| Kanpur | 15,900 | 5 |
| Lucknow | 79,000 | 3 |
| Mumbai | 65,000 | 1 |
| **Total** | **223,100** | **14** |

Duplicate order `O103` is counted once in cumulative sales.

## Generated Files
The `output/` and `checkpoint/` directories are generated at runtime and should not be committed to Git. The project's `.gitignore` excludes them.
