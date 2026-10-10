
import os
import glob

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    to_timestamp,
    sum as spark_sum,
    row_number,
    udf,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

SOURCE = "order_source"
CHECKPOINT = "checkpoint/order_stream"
BATCH_OUTPUT = "output/processed_batches"
SALES_OUTPUT = "output/city_sales"

# --------------------------------------------------
# 2. CREATE SPARK SESSION
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("VigneshIncrementalOrderSales")
    .config("spark.hadoop.fs.defaultFS", "file:///")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark version:", spark.version)

# --------------------------------------------------
# 3. DEFINE INPUT SCHEMA
# --------------------------------------------------

schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("order_timestamp", StringType(), True),
])

# --------------------------------------------------
# 4. BUSINESS CLASSIFICATION UDF
# --------------------------------------------------

def classify_order(value):
    if value is None:
        return "INVALID"
    elif value >= 50000:
        return "HIGH_VALUE"
    elif value >= 10000:
        return "MEDIUM_VALUE"
    else:
        return "LOW_VALUE"


classify_udf = udf(classify_order, StringType())

# --------------------------------------------------
# 5. READ NEW CSV FILES INCREMENTALLY
# --------------------------------------------------

orders = (
    spark.readStream
    .schema(schema)
    .option("header", True)
    .option("recursiveFileLookup", True)
    .option("maxFilesPerTrigger", 1)
    .csv(SOURCE)
)

# --------------------------------------------------
# 6. VALIDATE AND TRANSFORM ORDERS
# --------------------------------------------------

transformed = (
    orders
    .withColumn(
        "order_timestamp",
        to_timestamp(
            col("order_timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
    .filter(
        col("order_id").isNotNull()
        & (col("order_id") != "")
        & col("customer_id").isNotNull()
        & (col("customer_id") != "")
        & col("product").isNotNull()
        & (col("product") != "")
        & col("quantity").isNotNull()
        & (col("quantity") > 0)
        & col("unit_price").isNotNull()
        & (col("unit_price") >= 0)
        & col("city").isNotNull()
        & (col("city") != "")
        & col("order_timestamp").isNotNull()
    )
    .withColumn(
        "total_value",
        col("quantity") * col("unit_price")
    )
    .withColumn(
        "order_category",
        classify_udf(col("total_value"))
    )
)

# --------------------------------------------------
# 7. PROCESS EACH STREAMING MICRO-BATCH
# --------------------------------------------------

def process_batch(batch_df, batch_id):

    print(f"\nProcessing batch: {batch_id}")

    if batch_df.isEmpty():
        print("No valid orders in this batch.")
        return

    # Save each batch separately.
    # Retrying the same batch overwrites its own folder.
    batch_path = os.path.join(
        BATCH_OUTPUT,
        f"batch_{batch_id:08d}"
    )

    (
        batch_df
        .dropDuplicates(["order_id"])
        .write
        .mode("overwrite")
        .parquet(batch_path)
    )

    # Discover previously saved batch directories.
    batch_paths = [
        path
        for path in glob.glob(
            os.path.join(BATCH_OUTPUT, "batch_*")
        )
        if os.path.isdir(path)
        and glob.glob(os.path.join(path, "*.parquet"))
    ]

    if not batch_paths:
        print("No saved batch data found.")
        return

    # Load historical and newly processed orders.
    all_batches = spark.read.parquet(*batch_paths)

    # Resolve duplicate order IDs consistently:
    # choose the earliest timestamp, then sort by
    # customer, product and city for tied timestamps.
    dedup_window = (
        Window
        .partitionBy("order_id")
        .orderBy(
            col("order_timestamp").asc(),
            col("customer_id").asc(),
            col("product").asc(),
            col("city").asc(),
            col("quantity").asc(),
            col("unit_price").asc(),
        )
    )

    unique_orders = (
        all_batches
        .withColumn(
            "_row_number",
            row_number().over(dedup_window)
        )
        .filter(col("_row_number") == 1)
        .drop("_row_number")
    )

    # Save the cumulative city-wise sales.
    city_sales = (
        unique_orders
        .groupBy("city")
        .agg(
            spark_sum("total_value").alias("total_sales"),
            spark_sum("quantity").alias("total_items"),
        )
        .orderBy("city")
    )

    (
        city_sales
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(SALES_OUTPUT)
    )

    # Display the current cumulative result.
    print("Cumulative city-wise sales:")
    city_sales.show(truncate=False)

    print(f"Batch {batch_id} completed successfully.")


# --------------------------------------------------
# 8. START STREAM WITH CHECKPOINTING
# --------------------------------------------------

query = (
    transformed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

print("Streaming started. Waiting for incoming order files...")

query.awaitTermination()
