"""
ECS Validation Task for E-Commerce Data Pipeline

- Loads a Step Function trigger file from S3 describing a batch of order groups and their Parquet files.
- Validates schema, required fields, business logic, and unique key constraints for each file.
- Tracks validated files in a state file in S3.
- Logs all actions and errors to both stdout and S3.
"""

import json
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import logging
from datetime import datetime
from uuid import uuid4
from botocore.exceptions import ClientError
import time
from functools import wraps
from typing import Tuple, List, Dict, Any, Optional

# --- Configuration Variables ---

PROJECT_BUCKET = "lab-6-project"
LOGS_DATA_PREFIX = "logs/"
STATE_DATA_PREFIX = "state/"
TRIGGER_FILE_S3_PATH = "s3://lab-6-project/state/step_function_trigger_12345.json"
STATE_FILE_KEY = f"{STATE_DATA_PREFIX}validated_files_state.json"

EXPECTED_SCHEMAS = {
    "orders": ["order_id", "user_id", "status", "created_at", "returned_at", "shipped_at", "delivered_at", "num_of_item"],
    "order_items": ["id", "order_id", "user_id", "product_id", "status", "created_at", "shipped_at", "delivered_at", "returned_at", "sale_price"],
    "products": ["id", "sku", "cost", "category", "name", "brand", "retail_price", "department"]
}

UNIQUE_KEYS = {
    "orders": ["order_id"],
    "order_items": ["order_id", "product_id"],
    "products": ["id"]
}

REQUIRED_COLUMNS = {
    "orders": ["order_id", "user_id", "status", "created_at"],
    "order_items": ["id", "order_id", "user_id", "product_id", "status"],
    "products": ["id", "sku", "name", "brand"]
}

# --- AWS Clients ---
s3_client = boto3.client("s3")

# --- Logging Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    console_handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# In-memory buffer for S3 log upload
s3_log_buffer: List[str] = []

def log_and_buffer(level: str, message: str) -> None:
    """Log to console and buffer for S3 upload."""
    getattr(logger, level)(message)
    s3_log_buffer.append(f"[{datetime.now().isoformat()}] [{level.upper()}] {message}")

def upload_logs_to_s3(batch_id: str) -> None:
    """Upload accumulated logs to S3."""
    if not s3_log_buffer:
        return
    log_key = f"{LOGS_DATA_PREFIX}validation/{datetime.now().strftime('%Y/%m/%d')}/{batch_id}-{uuid4()}.log"
    try:
        s3_client.put_object(
            Bucket=PROJECT_BUCKET,
            Key=log_key,
            Body="\n".join(s3_log_buffer).encode("utf-8")
        )
        logger.info(f"S3 log written to s3://{PROJECT_BUCKET}/{log_key}")
    except Exception as e:
        logger.error(f"Failed to write logs to S3: {e}")

def retry_on_failure(max_retries: int = 3, delay: float = 1.0):
    """
    Decorator to retry a function on failure (ClientError, ArrowException, EmptyDataError).
    Exponential backoff applied.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except (ClientError, pa.ArrowException, pd.errors.EmptyDataError) as e:
                    if attempt == max_retries - 1:
                        log_and_buffer("error", f"Max retries reached for {func.__name__}: {e}")
                        raise
                    wait_time = delay * (2 ** attempt)
                    log_and_buffer("warning", f"Attempt {attempt + 1} failed for {func.__name__}: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
            return None
        return wrapper
    return decorator

def parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """
    Parse an S3 URI into (bucket, key).
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    path = s3_path[5:]
    bucket, key = path.split("/", 1)
    return bucket, key

@retry_on_failure(max_retries=3)
def s3_object_exists(bucket: str, key: str) -> bool:
    """
    Check if an S3 object exists.
    """
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise

def load_json_from_s3(s3_path: str) -> Dict[str, Any]:
    """
    Load a JSON file from S3.
    """
    bucket, key = parse_s3_path(s3_path)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

def save_json_to_s3(data: Dict[str, Any], bucket: str, key: str) -> None:
    """
    Save a dictionary as a JSON file to S3.
    """
    s3_client.put_object(Bucket=bucket, Key=key, Body=json.dumps(data, indent=2).encode('utf-8'))
    log_and_buffer("info", f"Saved JSON to s3://{bucket}/{key}")

def load_state_file() -> Dict[str, Any]:
    """
    Load the validation state file from S3, or initialize if not found.
    """
    bucket = PROJECT_BUCKET
    key = STATE_FILE_KEY
    if s3_object_exists(bucket, key):
        log_and_buffer("info", f"Loading state file s3://{bucket}/{key}")
        try:
            return load_json_from_s3(f"s3://{bucket}/{key}")
        except Exception as e:
            log_and_buffer("error", f"Failed to load state file: {e}")
            return {"validated_files": []}
    else:
        log_and_buffer("info", "State file not found, initializing new state.")
        return {"validated_files": []}

def update_state_file(validated_files: List[str]) -> None:
    """
    Update the validation state file in S3.
    """
    bucket = PROJECT_BUCKET
    key = STATE_FILE_KEY
    state_data = {"validated_files": validated_files}
    save_json_to_s3(state_data, bucket, key)
    log_and_buffer("info", f"State file updated with {len(validated_files)} validated files.")

def validate_parquet_header(s3_path: str, expected_columns: List[str]) -> Tuple[bool, List[str]]:
    """
    Validate the Parquet file header for required columns and duplicates.
    Returns (is_valid, [issues]).
    """
    issues: List[str] = []
    try:
        bucket, key = parse_s3_path(s3_path)
        if not s3_object_exists(bucket, key):
            issues.append(f"File does not exist: {s3_path}")
            return False, issues
        # Download the file into memory (safe for moderate file sizes)
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
        data = body.read()
        parquet_file = pq.ParquetFile(pa.BufferReader(data))
        actual_columns = parquet_file.schema.names
        missing_cols = set(expected_columns) - set(actual_columns)
        if missing_cols:
            issues.append(f"Missing columns: {missing_cols}")
        if len(actual_columns) != len(set(actual_columns)):
            duplicates = [col for col in actual_columns if actual_columns.count(col) > 1]
            issues.append(f"Duplicate columns: {set(duplicates)}")
        return len(issues) == 0, issues
    except Exception as e:
        issues.append(f"Error reading parquet header: {e}")
        return False, issues

def validate_parquet_content(s3_path: str, file_type: str, order_id: str) -> Tuple[bool, List[str]]:
    """
    Validate Parquet file content for required fields, business logic, and unique key constraints.
    Returns (is_valid, [issues]).
    """
    issues: List[str] = []
    try:
        bucket, key = parse_s3_path(s3_path)
        body = s3_client.get_object(Bucket=bucket, Key=key)["Body"]
        data = body.read()
        parquet_file = pq.ParquetFile(pa.BufferReader(data))
        df = parquet_file.read().to_pandas()
        # Filter by order_id if applicable
        if file_type != "products" and "order_id" in df.columns:
            df = df[df["order_id"].astype(str) == order_id]
        if df.empty:
            issues.append(f"No data for order_id {order_id}")
            return False, issues
        # Check for nulls and duplicates in unique keys
        unique_keys = UNIQUE_KEYS.get(file_type, [])
        for key_col in unique_keys:
            if key_col in df.columns:
                if df[key_col].isnull().any():
                    issues.append(f"Null values found in primary key column '{key_col}'")
                if df[key_col].duplicated().any():
                    issues.append(f"Duplicate values found in primary key column '{key_col}'")
        # Business rules validation
        if file_type == "orders" and "status" in df.columns:
            valid_statuses = ["pending", "processing", "shipped", "delivered", "cancelled", "returned"]
            invalid_statuses = df[~df["status"].isin(valid_statuses)]["status"].unique()
            if len(invalid_statuses) > 0:
                issues.append(f"Invalid statuses: {invalid_statuses}")
        if file_type == "order_items" and "sale_price" in df.columns:
            if (df["sale_price"] < 0).any():
                issues.append(f"Negative sale_price values found")
        if file_type == "products":
            if "cost" in df.columns and (df["cost"] < 0).any():
                issues.append(f"Negative cost values found")
            if "retail_price" in df.columns and (df["retail_price"] < 0).any():
                issues.append(f"Negative retail_price values found")
        # Type checks (optional, for robustness)
        # Example: check if 'num_of_item' is int for orders
        if file_type == "orders" and "num_of_item" in df.columns:
            if not pd.api.types.is_integer_dtype(df["num_of_item"]):
                issues.append("num_of_item is not integer type")
        return len(issues) == 0, issues
    except Exception as e:
        issues.append(f"Error reading parquet content: {e}")
        return False, issues

def validate_file(s3_path: str, file_type: str, order_id: str) -> Tuple[bool, List[str]]:
    """
    Validate a single file for schema, content, and business rules.
    Returns (is_valid, [issues]).
    """
    log_and_buffer("info", f"Validating file {s3_path} for order_id {order_id}")
    expected_cols = EXPECTED_SCHEMAS.get(file_type, [])
    header_valid, header_issues = validate_parquet_header(s3_path, expected_cols)
    if not header_valid:
        log_and_buffer("error", f"Header validation failed for {s3_path}: {header_issues}")
        return False, header_issues
    content_valid, content_issues = validate_parquet_content(s3_path, file_type, order_id)
    if not content_valid:
        log_and_buffer("error", f"Content validation failed for {s3_path}: {content_issues}")
        return False, content_issues
    log_and_buffer("info", f"File {s3_path} passed validation")
    return True, []

def main() -> None:
    """
    Main validation workflow:
    - Loads trigger file and state file from S3
    - Validates each referenced Parquet file
    - Updates state file and logs results to S3
    """
    log_and_buffer("info", "Starting validation job")
    batch_id: str = "unknown"
    try:
        trigger_data = load_json_from_s3(TRIGGER_FILE_S3_PATH)
        log_and_buffer("info", f"Loaded trigger file from {TRIGGER_FILE_S3_PATH}")
        batch_id = trigger_data.get("batch_id", f"batch_{uuid4()}")
    except Exception as e:
        log_and_buffer("error", f"Failed to load trigger file: {e}")
        upload_logs_to_s3(batch_id)
        return

    try:
        state = load_state_file()
        validated_files = set(state.get("validated_files", []))
    except Exception as e:
        log_and_buffer("error", f"Failed to load state file: {e}")
        validated_files = set()

    overall_status = "SUCCESS"
    validation_results: List[Dict[str, Any]] = []

    for group in trigger_data.get("groups", []):
        order_id = group.get("order_id", f"order_{uuid4()}")
        group_status = "SUCCESS"
        group_issues: List[str] = []
        for file_info in group.get("files", []):
            s3_path = file_info.get("path")
            file_type = file_info.get("type")
            if not s3_path or not file_type:
                log_and_buffer("warning", f"Skipping invalid file info: {file_info}")
                continue
            if s3_path in validated_files:
                log_and_buffer("info", f"Skipping already validated file: {s3_path}")
                continue
            try:
                valid, issues = validate_file(s3_path, file_type, order_id)
                if not valid:
                    group_status = "FAILURE"
                    overall_status = "FAILURE"
                    group_issues.extend(issues)
                else:
                    validated_files.add(s3_path)
            except Exception as e:
                group_status = "FAILURE"
                overall_status = "FAILURE"
                group_issues.append(f"Exception during validation: {e}")
                log_and_buffer("error", f"Exception during validation of {s3_path}: {e}")
        validation_results.append({
            "order_id": order_id,
            "status": group_status,
            "issues": group_issues if group_issues else None
        })

    try:
        update_state_file(list(validated_files))
    except Exception as e:
        log_and_buffer("error", f"Failed to update state file: {e}")

    log_and_buffer("info", f"Validation batch {batch_id} completed with status: {overall_status}")
    for result in validation_results:
        log_and_buffer("info", f"Order {result['order_id']} validation status: {result['status']}")
        if result['issues']:
            for issue in result['issues']:
                log_and_buffer("error", f"Issue: {issue}")

    upload_logs_to_s3(batch_id)

if __name__ == "__main__":
    main()
