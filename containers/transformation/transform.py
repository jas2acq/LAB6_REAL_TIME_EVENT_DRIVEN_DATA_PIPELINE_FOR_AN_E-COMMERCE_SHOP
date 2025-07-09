"""
ECS Transformation Task for E-Commerce Analytics Pipeline

This script is intended to run as a containerized ECS task, orchestrated by AWS Step Functions.
It reads validated Parquet data for orders, order_items, and products from S3, computes business KPIs,
and writes the results to DynamoDB for real-time analytics.

--------------------------------------------------------
DATA FORMAT & SAMPLE SCHEMA

Input: Parquet files for 'orders', 'order_items', and 'products', each with the following columns:

orders:
    order_id (string), user_id (string), status (string), created_at (datetime),
    returned_at (datetime, nullable), shipped_at (datetime, nullable),
    delivered_at (datetime, nullable), num_of_item (int)

order_items:
    id (string), order_id (string), user_id (string), product_id (string),
    status (string), created_at (datetime), shipped_at (datetime, nullable),
    delivered_at (datetime, nullable), returned_at (datetime, nullable), sale_price (float)

products:
    id (string), sku (string), cost (float), category (string), name (string),
    brand (string), retail_price (float), department (string)

--------------------------------------------------------
VALIDATION RULES

- Assumes all input data has been validated by a previous ECS task.
- Handles missing or corrupt files gracefully with logging.

--------------------------------------------------------
DYNAMODB TABLE STRUCTURE

Category-Level KPIs Table (ecom_category_kpis):
    PK: category (string)
    SK: order_date (string, YYYY-MM-DD)
    daily_revenue (float), avg_order_value (float), avg_return_rate (float)

Order-Level KPIs Table (ecom_order_kpis_daily):
    PK: order_date (string, YYYY-MM-DD)
    total_orders (int), total_revenue (float), total_items_sold (int),
    return_rate (float), unique_customers (int)

--------------------------------------------------------
STEP FUNCTION WORKFLOW

1. S3 Event triggers Step Function.
2. Step Function runs ECS Validation Task.
3. If valid, Step Function runs this ECS Transformation Task.
4. KPIs are written to DynamoDB.
5. Logs are written to CloudWatch and S3.

--------------------------------------------------------
ERROR HANDLING, RETRY, AND LOGGING LOGIC

- All steps are wrapped in try/except blocks.
- Errors are logged to both stdout and S3.
- DynamoDB writes use retry logic.
- Any failure triggers pipeline failure/notification via Step Functions.

--------------------------------------------------------
INSTRUCTIONS TO SIMULATE OR TEST

1. Upload validated Parquet files to S3.
2. Create a trigger file referencing these files.
3. Run this ECS task with the trigger file S3 path as an argument:
    spark-submit transformation.py s3://your-bucket/state/step_function_trigger_xxx.json
4. Check logs in CloudWatch and S3.
5. Verify output in DynamoDB tables.

--------------------------------------------------------
"""

import sys
import json
import boto3
import logging
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, avg as spark_avg, countDistinct, count, when
)
from pyspark.sql.types import StringType
from botocore.exceptions import ClientError

# --- Configuration Variables (edit as needed) ---
CATEGORY_KPI_TABLE = "ecom_category_kpis"
ORDER_KPI_TABLE = "ecom_order_kpis_daily"
PROJECT_BUCKET = "lab-6-project"
LOGS_DATA_PREFIX = "logs/"
DYNAMODB_REGION = "eu-north-1"  # Change as needed

# --- Logging Setup ---
logger = logging.getLogger("transformation")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def log_and_buffer(level, message, s3_log_buffer):
    """Log to stdout and buffer for S3 log upload."""
    getattr(logger, level)(message)
    s3_log_buffer.append(f"[{datetime.now().isoformat()}] [{level.upper()}] {message}")

def upload_logs_to_s3(logs, batch_id):
    """Upload accumulated logs to S3."""
    if not logs:
        return
    s3 = boto3.client("s3")
    log_key = f"{LOGS_DATA_PREFIX}transformation/{datetime.now().strftime('%Y/%m/%d')}/{batch_id}-{datetime.now().strftime('%H%M%S')}.log"
    try:
        s3.put_object(
            Bucket=PROJECT_BUCKET,
            Key=log_key,
            Body="\n".join(logs).encode("utf-8")
        )
        logger.info(f"S3 log written to s3://{PROJECT_BUCKET}/{log_key}")
    except Exception as e:
        logger.error(f"Failed to write logs to S3: {e}")

def read_parquet(spark: SparkSession, s3_path: str) -> DataFrame:
    """Read a Parquet file from S3 into a Spark DataFrame."""
    try:
        return spark.read.parquet(s3_path)
    except Exception as e:
        logger.error(f"Failed to read Parquet file {s3_path}: {e}")
        raise

def main(trigger_file_path: str):
    """
    Main transformation workflow.
    Reads validated Parquet files, computes KPIs, and writes results to DynamoDB.
    """
    s3_log_buffer = []
    batch_id = None
    try:
        # --- Start Spark session ---
        spark = SparkSession.builder.appName("ECS-Transformation").getOrCreate()
        log_and_buffer("info", "Spark session started.", s3_log_buffer)

        # --- Load trigger file from S3 ---
        s3 = boto3.client("s3")
        bucket, key = trigger_file_path.replace("s3://", "").split("/", 1)
        trigger_obj = s3.get_object(Bucket=bucket, Key=key)
        trigger_data = json.loads(trigger_obj["Body"].read().decode("utf-8"))
        batch_id = trigger_data.get("batch_id", f"batch_{datetime.now().strftime('%Y%m%dT%H%M%S')}")
        log_and_buffer("info", f"Loaded trigger file: {trigger_file_path}", s3_log_buffer)

        # --- Collect file paths from trigger file ---
        orders_path = None
        order_items_path = None
        products_path = None
        for group in trigger_data.get("groups", []):
            for file_info in group.get("files", []):
                if file_info["type"] == "orders":
                    orders_path = file_info["path"]
                elif file_info["type"] == "order_items":
                    order_items_path = file_info["path"]
                elif file_info["type"] == "products":
                    products_path = file_info["path"]
        if not (orders_path and order_items_path and products_path):
            raise ValueError("Missing one or more required file paths in trigger file.")

        # --- Read data from S3 ---
        orders = read_parquet(spark, orders_path)
        order_items = read_parquet(spark, order_items_path)
        products = read_parquet(spark, products_path)
        log_and_buffer("info", "Loaded Parquet files from S3.", s3_log_buffer)

        # --- Date Preparation ---
        orders = orders.withColumn("created_at", col("created_at").cast("timestamp"))
        orders = orders.withColumn("order_date", col("created_at").cast("date").cast(StringType()))
        order_items = order_items.withColumn("created_at", col("created_at").cast("timestamp"))

        # --- Integrate Data ---
        # Join orders and order_items on order_id, then join with products on product_id
        oi = order_items.join(
            orders.select("order_id", "order_date", "status", "user_id", "num_of_item"),
            on="order_id", how="inner"
        )
        full = oi.join(
            products.select(
                col("id").alias("product_id"),
                "category", "cost", "retail_price"
            ),
            on="product_id", how="inner"
        )

        # --- Category-Level KPIs ---
        cat_kpis = (
            full.groupBy("category", "order_date")
            .agg(
                spark_sum("sale_price").alias("daily_revenue"),
                spark_avg("sale_price").alias("avg_order_value"),
                (spark_sum(when(col("status") == "returned", 1).otherwise(0)) / count("order_id")).alias("avg_return_rate")
            )
        )

        # --- Order-Level KPIs ---
        order_kpis = (
            full.groupBy("order_date")
            .agg(
                countDistinct("order_id").alias("total_orders"),
                spark_sum("sale_price").alias("total_revenue"),
                spark_sum("num_of_item").alias("total_items_sold"),
                (spark_sum(when(col("status") == "returned", 1).otherwise(0)) / countDistinct("order_id")).alias("return_rate"),
                countDistinct("user_id").alias("unique_customers")
            )
        )

        # --- Write KPIs to DynamoDB with error handling and retries ---
        dynamodb = boto3.resource("dynamodb", region_name=DYNAMODB_REGION)
        cat_table = dynamodb.Table(CATEGORY_KPI_TABLE)
        order_table = dynamodb.Table(ORDER_KPI_TABLE)

        # Write Category-Level KPIs
        for row in cat_kpis.collect():
            item = {
                "category": row["category"],
                "order_date": row["order_date"],
                "daily_revenue": float(row["daily_revenue"]) if row["daily_revenue"] is not None else 0.0,
                "avg_order_value": float(row["avg_order_value"]) if row["avg_order_value"] is not None else 0.0,
                "avg_return_rate": float(row["avg_return_rate"]) if row["avg_return_rate"] is not None else 0.0,
            }
            for attempt in range(3):
                try:
                    cat_table.put_item(Item=item)
                    break
                except Exception as e:
                    log_and_buffer("error", f"Error writing category KPI to DynamoDB: {e}", s3_log_buffer)
                    if attempt == 2:
                        raise

        # Write Order-Level KPIs
        for row in order_kpis.collect():
            item = {
                "order_date": row["order_date"],
                "total_orders": int(row["total_orders"]) if row["total_orders"] is not None else 0,
                "total_revenue": float(row["total_revenue"]) if row["total_revenue"] is not None else 0.0,
                "total_items_sold": int(row["total_items_sold"]) if row["total_items_sold"] is not None else 0,
                "return_rate": float(row["return_rate"]) if row["return_rate"] is not None else 0.0,
                "unique_customers": int(row["unique_customers"]) if row["unique_customers"] is not None else 0,
            }
            for attempt in range(3):
                try:
                    order_table.put_item(Item=item)
                    break
                except Exception as e:
                    log_and_buffer("error", f"Error writing order KPI to DynamoDB: {e}", s3_log_buffer)
                    if attempt == 2:
                        raise

        log_and_buffer("info", "Transformation and DynamoDB write complete.", s3_log_buffer)

    except Exception as e:
        log_and_buffer("error", f"Transformation failed: {e}", s3_log_buffer)
        raise
    finally:
        upload_logs_to_s3(s3_log_buffer, batch_id or "unknown")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: spark-submit transformation.py <trigger_file_s3_path>")
        sys.exit(1)
    trigger_file_path = sys.argv[1]
    main(trigger_file_path)
