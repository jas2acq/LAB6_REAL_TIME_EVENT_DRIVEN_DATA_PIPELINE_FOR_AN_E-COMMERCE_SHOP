"""
ECS Transformation Task for E-Commerce Data Pipeline

- Loads all Step Function trigger files from S3 describing batches of order groups and their Parquet files.
- Only processes files that are validated (per validated_files_state.json) and not yet transformed (per transformed_files_state.json).
- Tracks transformed files in a state file in S3.
- Logs all actions and errors to both stdout and S3.
- Performs data integration and KPI computations.
- Stores KPIs in Delta Lake with upsert semantics.
- Pushes KPIs to DynamoDB with batch writes and retries.
"""

import json
import boto3
import logging
from datetime import datetime
from uuid import uuid4
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, sum as spark_sum, avg, countDistinct, count, when
from pyspark.sql.types import DateType
import sys
import time
from delta import configure_spark_with_delta_pip
from decimal import Decimal
from delta.tables import DeltaTable



# --- Configuration Variables ---
PROJECT_BUCKET = "lab-6-project"
LOGS_DATA_PREFIX = "logs/"
STATE_DATA_PREFIX = "state/"
TRIGGER_FILE_PREFIX = "step_function_trigger_"
VALIDATED_STATE_KEY = f"{STATE_DATA_PREFIX}validated_files_state.json"
TRANSFORMED_STATE_KEY = f"{STATE_DATA_PREFIX}transformed_files_state.json"

DELTA_CATEGORY_KPIS_PATH = f"s3a://{PROJECT_BUCKET}/delta/category_kpis/"
DELTA_ORDER_KPIS_PATH = f"s3a://{PROJECT_BUCKET}/delta/order_kpis_daily/"

DYNAMODB_CATEGORY_TABLE = "ecom_category_kpis"
DYNAMODB_ORDER_TABLE = "ecom_order_kpis_daily"

# --- AWS Clients ---
s3_client = boto3.client("s3")
dynamodb = boto3.resource('dynamodb')
category_kpi_table = dynamodb.Table(DYNAMODB_CATEGORY_TABLE)
order_kpi_table = dynamodb.Table(DYNAMODB_ORDER_TABLE)

# --- Logging Setup ---
logger = logging.getLogger("transformation")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

s3_log_buffer = []

def log_and_buffer(level: str, message: str) -> None:
    getattr(logger, level)(message)
    s3_log_buffer.append(f"[{datetime.now().isoformat()}] [{level.upper()}] {message}")

def upload_logs_to_s3(batch_id: str) -> None:
    if not s3_log_buffer:
        return
    log_key = f"{LOGS_DATA_PREFIX}transformation/{datetime.now().strftime('%Y/%m/%d')}/{batch_id}-{uuid4()}.log"
    try:
        s3_client.put_object(
            Bucket=PROJECT_BUCKET,
            Key=log_key,
            Body="\n".join(s3_log_buffer).encode("utf-8")
        )
        logger.info(f"S3 log written to s3://{PROJECT_BUCKET}/{log_key}")
    except Exception as e:
        logger.error(f"Failed to write logs to S3: {e}")

def parse_s3_path(s3_path: str) -> tuple:
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    path = s3_path[5:]
    bucket, key = path.split("/", 1)
    return bucket, key

def get_all_trigger_files(bucket: str, prefix: str) -> list:
    trigger_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.json') and TRIGGER_FILE_PREFIX in key:
                    trigger_files.append(f"s3://{bucket}/{key}")
    except ClientError as e:
        log_and_buffer("error", f"Error listing trigger files in s3://{bucket}/{prefix}: {e}")
    return trigger_files

def get_validated_files() -> set:
    try:
        obj = s3_client.get_object(Bucket=PROJECT_BUCKET, Key=VALIDATED_STATE_KEY)
        state = json.loads(obj["Body"].read())
        return set(state.get("validated_files", []))
    except s3_client.exceptions.NoSuchKey:
        log_and_buffer("warning", "No validated state file found, assuming no files validated yet.")
        return set()
    except Exception as e:
        log_and_buffer("error", f"Error loading validated state file: {e}")
        return set()
    

def table_exists(table_name: str) -> bool:
    """
    Check if a DynamoDB table exists and is ACTIVE.
    """
    try:
        response = dynamodb.meta.client.describe_table(TableName=table_name)
        return response['Table']['TableStatus'] == 'ACTIVE'
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        return False
    except Exception as e:
        log_and_buffer("error", f"Error checking table {table_name} existence: {e}")
        return False


def create_category_kpi_table():
    """
    Create DynamoDB category KPI table if it doesn't exist.
    """
    if table_exists(DYNAMODB_CATEGORY_TABLE):
        log_and_buffer("info", f"Table {DYNAMODB_CATEGORY_TABLE} already exists.")
        return

    params = {
        "TableName": DYNAMODB_CATEGORY_TABLE,
        "KeySchema": [
            {"AttributeName": "category", "KeyType": "HASH"},
            {"AttributeName": "order_date", "KeyType": "RANGE"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "category", "AttributeType": "S"},
            {"AttributeName": "order_date", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST"
    }

    try:
        table = dynamodb.create_table(**params)
        log_and_buffer("info", f"Creating table {DYNAMODB_CATEGORY_TABLE}...")
        table.wait_until_exists()
        log_and_buffer("info", f"Table {DYNAMODB_CATEGORY_TABLE} is now ACTIVE.")
    except Exception as e:
        log_and_buffer("error", f"Failed to create table {DYNAMODB_CATEGORY_TABLE}: {e}")


def create_order_kpi_table():
    """
    Create DynamoDB order KPI table if it doesn't exist.
    """
    if table_exists(DYNAMODB_ORDER_TABLE):
        log_and_buffer("info", f"Table {DYNAMODB_ORDER_TABLE} already exists.")
        return

    params = {
        "TableName": DYNAMODB_ORDER_TABLE,
        "KeySchema": [
            {"AttributeName": "order_date", "KeyType": "HASH"}
        ],
        "AttributeDefinitions": [
            {"AttributeName": "order_date", "AttributeType": "S"}
        ],
        "BillingMode": "PAY_PER_REQUEST"
    }

    try:
        table = dynamodb.create_table(**params)
        log_and_buffer("info", f"Creating table {DYNAMODB_ORDER_TABLE}...")
        table.wait_until_exists()
        log_and_buffer("info", f"Table {DYNAMODB_ORDER_TABLE} is now ACTIVE.")
    except Exception as e:
        log_and_buffer("error", f"Failed to create table {DYNAMODB_ORDER_TABLE}: {e}")


def ensure_kpi_tables_exist():
    """
    Ensure both DynamoDB KPI tables exist.
    """
    create_category_kpi_table()
    create_order_kpi_table()



def get_transformed_files() -> set:
    try:
        obj = s3_client.get_object(Bucket=PROJECT_BUCKET, Key=TRANSFORMED_STATE_KEY)
        state = json.loads(obj["Body"].read())
        return set(state.get("transformed_files", []))
    except s3_client.exceptions.NoSuchKey:
        log_and_buffer("info", "No transformed state file found, starting fresh.")
        return set()
    except Exception as e:
        log_and_buffer("error", f"Error loading transformed state file: {e}")
        return set()

def update_transformed_files(files_set: set) -> None:
    try:
        s3_client.put_object(
            Bucket=PROJECT_BUCKET,
            Key=TRANSFORMED_STATE_KEY,
            Body=json.dumps({"transformed_files": list(files_set)}, indent=2).encode("utf-8")
        )
        log_and_buffer("info", f"Updated transformed state file with {len(files_set)} files.")
    except Exception as e:
        log_and_buffer("error", f"Failed to update transformed state file: {e}")

def load_json_from_s3(s3_path: str) -> dict:
    bucket, key = parse_s3_path(s3_path)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        log_and_buffer("error", f"Failed to load JSON from {s3_path}: {e}")
        raise

def batch_write_to_dynamodb(table, items, max_batch_size=25, max_retries=3):
    """
    Batch write items to DynamoDB with retries.

    Args:
        table: DynamoDB Table resource
        items: List of dict items to write
        max_batch_size: Max items per batch (DynamoDB limit is 25)
        max_retries: Number of retries for unprocessed items
    """
    from botocore.exceptions import ClientError
    import time

    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i:i + n]

    for batch in chunks(items, max_batch_size):
        retries = 0
        unprocessed = batch
        while unprocessed and retries <= max_retries:
            try:
                with table.batch_writer() as batch_writer:
                    for item in unprocessed:
                        batch_writer.put_item(Item=item)
                unprocessed = []
            except ClientError as e:
                log_and_buffer("warning", f"DynamoDB batch write error: {e}, retrying...")
                retries += 1
                time.sleep(2 ** retries)
        if unprocessed:
            log_and_buffer("error", f"Failed to write some items to DynamoDB after {max_retries} retries.")


def transform_and_compute_kpis(spark: SparkSession, files_to_transform: set) -> set:
    """
    Perform the transformation and KPI computation on validated files.

    Steps:
    - Load orders, order_items, and products parquet datasets from S3.
    - Join datasets to create enriched order-level data.
    - Write the transformed joined data as Delta Lake table representing the delta.
    - Compute category-level and order-level KPIs.
    - Push KPIs to DynamoDB using batch writes with retries.

    Args:
        spark (SparkSession): SparkSession object with Delta Lake support.
        files_to_transform (set): Set of S3 paths to transform.

    Returns:
        set: Set of successfully transformed file paths.
    """
    transformed_successfully = set()

    # Separate files by type
    orders_files = [f for f in files_to_transform if '/orders/' in f]
    order_items_files = [f for f in files_to_transform if '/order_items/' in f]
    products_files = [f for f in files_to_transform if '/products/' in f]

    # Helper to load parquet files from S3 paths
    def load_parquet_files(file_list):
        if not file_list:
            return None
        spark_paths = [f.replace("s3://", "s3a://") for f in file_list]
        return spark.read.parquet(*spark_paths)

    try:
        orders_df = load_parquet_files(orders_files)
        order_items_df = load_parquet_files(order_items_files)
        products_df = load_parquet_files(products_files)
    except Exception as e:
        log_and_buffer("error", f"Error loading parquet files: {e}")
        return transformed_successfully

    if orders_df is None or order_items_df is None or products_df is None:
        log_and_buffer("error", "One or more datasets are missing; skipping transformation.")
        return transformed_successfully

    # Prepare date columns
    orders_df = orders_df.withColumn("order_date", to_date(col("created_at")))
    order_items_df = order_items_df.withColumn("order_date", to_date(col("created_at")))
    # products_df does not require date column

    # Join datasets to create enriched order data
    try:
        joined_df = orders_df.alias("o") \
            .join(order_items_df.alias("oi"), col("o.order_id") == col("oi.order_id"), "inner") \
            .join(products_df.alias("p"), col("oi.product_id") == col("p.id"), "inner") \
            .select(
                col("o.order_id"),
                col("o.user_id"),
                col("o.status"),
                col("o.order_date"),
                col("oi.sale_price"),
                col("oi.product_id"),
                col("p.category"),
                col("oi.returned_at")
            )
    except Exception as e:
        log_and_buffer("error", f"Error joining dataframes: {e}")
        return transformed_successfully

    # Write transformed joined data as Delta Lake (delta)
    DELTA_TRANSFORMED_PATH = f"s3a://{PROJECT_BUCKET}/delta/transformed_orders/"
    try:
        if DeltaTable.isDeltaTable(spark, DELTA_TRANSFORMED_PATH):
            delta_transformed = DeltaTable.forPath(spark, DELTA_TRANSFORMED_PATH)
            delta_transformed.alias("target").merge(
                joined_df.alias("source"),
                "target.order_id = source.order_id AND target.product_id = source.product_id"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            # Initial write: overwrite mode with partition by order_date
            joined_df.write.format("delta").mode("overwrite").partitionBy("order_date").save(DELTA_TRANSFORMED_PATH)

        log_and_buffer("info", "Transformed data written/upserted to Delta Lake as delta.")
    except Exception as e:
        log_and_buffer("error", f"Error writing transformed data to Delta Lake: {e}")
        return transformed_successfully

    # Compute Category-Level KPIs
    try:
        category_kpis = joined_df.groupBy("category", "order_date").agg(
            spark_sum("sale_price").alias("daily_revenue"),
            avg("sale_price").alias("avg_order_value"),
            (spark_sum(when(col("returned_at").isNotNull(), 1).otherwise(0)) / count("order_id")).alias("avg_return_rate")
        )
    except Exception as e:
        log_and_buffer("error", f"Error computing category-level KPIs: {e}")
        return transformed_successfully

    # Compute Order-Level KPIs
    try:
        order_kpis = joined_df.groupBy("order_date").agg(
            countDistinct("order_id").alias("total_orders"),
            spark_sum("sale_price").alias("total_revenue"),
            count("product_id").alias("total_items_sold"),
            (spark_sum(when(col("returned_at").isNotNull(), 1).otherwise(0)) / countDistinct("order_id")).alias("return_rate"),
            countDistinct("user_id").alias("unique_customers")
        )
    except Exception as e:
        log_and_buffer("error", f"Error computing order-level KPIs: {e}")
        return transformed_successfully
    
        # Ensure DynamoDB tables exist before pushing KPIs
    ensure_kpi_tables_exist()

    # Push KPIs to DynamoDB with Decimal conversion for numeric types
    try:
        # Category KPIs
        cat_kpi_items = category_kpis.collect()
        cat_items_to_write = []
        for row in cat_kpi_items:
            cat_items_to_write.append({
                "category": row["category"],
                "order_date": row["order_date"].strftime("%Y-%m-%d"),
                "daily_revenue": Decimal(str(row["daily_revenue"])) if row["daily_revenue"] is not None else Decimal('0'),
                "avg_order_value": Decimal(str(row["avg_order_value"])) if row["avg_order_value"] is not None else Decimal('0'),
                "avg_return_rate": Decimal(str(row["avg_return_rate"])) if row["avg_return_rate"] is not None else Decimal('0')
            })

        batch_write_to_dynamodb(category_kpi_table, cat_items_to_write)

        # Order KPIs
        ord_kpi_items = order_kpis.collect()
        ord_items_to_write = []
        for row in ord_kpi_items:
            ord_items_to_write.append({
                "order_date": row["order_date"].strftime("%Y-%m-%d"),
                "total_orders": int(row["total_orders"]) if row["total_orders"] is not None else 0,
                "total_revenue": Decimal(str(row["total_revenue"])) if row["total_revenue"] is not None else Decimal('0'),
                "total_items_sold": int(row["total_items_sold"]) if row["total_items_sold"] is not None else 0,
                "return_rate": Decimal(str(row["return_rate"])) if row["return_rate"] is not None else Decimal('0'),
                "unique_customers": int(row["unique_customers"]) if row["unique_customers"] is not None else 0
            })

        batch_write_to_dynamodb(order_kpi_table, ord_items_to_write)

        log_and_buffer("info", "KPIs pushed to DynamoDB.")
    except Exception as e:
        log_and_buffer("error", f"Error pushing KPIs to DynamoDB: {e}")
        return transformed_successfully


def main():
    log_and_buffer("info", "Starting transformation job")
    batch_id = f"batch_{uuid4()}"

    try:
        # Configure Spark with Delta Lake extensions explicitly
        builder = SparkSession.builder.appName("ECS-Transformation") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com")

        # Use configure_spark_with_delta_pip to add Delta packages
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        log_and_buffer("info", "Spark session started with Delta Lake support.")
    except Exception as e:
        log_and_buffer("error", f"Failed to start Spark session: {e}")
        upload_logs_to_s3(batch_id)
        return

    try:
        validated_files = get_validated_files()
        transformed_files = get_transformed_files()
        files_to_transform = validated_files - transformed_files
        log_and_buffer("info", f"{len(validated_files)} validated files, {len(transformed_files)} already transformed, {len(files_to_transform)} to transform.")
    except Exception as e:
        log_and_buffer("error", f"Failed to load state files: {e}")
        upload_logs_to_s3(batch_id)
        return

    if not files_to_transform:
        log_and_buffer("info", "No new files to transform. Exiting.")
        upload_logs_to_s3(batch_id)
        return

    # Retrieve all trigger files
    trigger_files = get_all_trigger_files(PROJECT_BUCKET, STATE_DATA_PREFIX + TRIGGER_FILE_PREFIX)
    if not trigger_files:
        log_and_buffer("warning", "No trigger files found to process.")
        upload_logs_to_s3(batch_id)
        return

    # Process each trigger file and transform
    transformed_files_set = set()
    for trigger_file_path in trigger_files:
        try:
            trigger_data = load_json_from_s3(trigger_file_path)
            log_and_buffer("info", f"Loaded trigger file from {trigger_file_path}")
            groups = trigger_data.get("groups", [])
        except Exception as e:
            log_and_buffer("error", f"Failed to load/parse trigger file {trigger_file_path}: {e}")
            continue

        # Collect files in this trigger that need transformation
        trigger_files_to_transform = set()
        for group in groups:
            for file_info in group.get("files", []):
                s3_path = file_info.get("path")
                if s3_path and s3_path in files_to_transform:
                    trigger_files_to_transform.add(s3_path)

        if not trigger_files_to_transform:
            log_and_buffer("info", f"No new files to transform in trigger {trigger_file_path}")
            continue

        # Perform transformation and KPI computation
        transformed = transform_and_compute_kpis(spark, trigger_files_to_transform)
        transformed_files_set.update(transformed)

    # Update transformed files state once after all triggers processed
    if transformed_files_set:
        try:
            transformed_files.update(transformed_files_set)
            update_transformed_files(transformed_files)
        except Exception as e:
            log_and_buffer("error", f"Failed to update transformed files state: {e}")

    upload_logs_to_s3(batch_id)

if __name__ == "__main__":
    main()
