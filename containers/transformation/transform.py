"""
ECS Transformation Task for E-Commerce Data Pipeline

- Loads all Step Function trigger files from S3 describing batches of order groups and their Parquet files.
- Only processes files that are validated (per validated_files_state.json) and not yet transformed (per transformed_files_state.json).
- Tracks transformed files in a state file in S3.
- Logs all actions and errors to both stdout and S3.
"""

import json
import boto3
import logging
from datetime import datetime
from uuid import uuid4
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
import sys

# --- Configuration Variables ---
PROJECT_BUCKET = "lab-6-project"
LOGS_DATA_PREFIX = "logs/"
STATE_DATA_PREFIX = "state/"
TRIGGER_FILE_PREFIX = "step_function_trigger_"
VALIDATED_STATE_KEY = f"{STATE_DATA_PREFIX}validated_files_state.json"
TRANSFORMED_STATE_KEY = f"{STATE_DATA_PREFIX}transformed_files_state.json"

# --- AWS Clients ---
s3_client = boto3.client("s3")

# --- Logging Setup ---
logger = logging.getLogger("transformation")
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# In-memory buffer to accumulate logs before uploading to S3
s3_log_buffer = []

def log_and_buffer(level: str, message: str) -> None:
    """
    Logs a message to console and buffers it for later upload to S3.

    Args:
        level (str): Logging level ('info', 'warning', 'error', etc.)
        message (str): Message to log
    """
    getattr(logger, level)(message)
    s3_log_buffer.append(f"[{datetime.now().isoformat()}] [{level.upper()}] {message}")

def upload_logs_to_s3(batch_id: str) -> None:
    """
    Uploads accumulated logs to S3 under a timestamped key.

    Args:
        batch_id (str): Identifier for the current batch/job
    """
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
    """
    Parses an S3 URI into bucket and key components.

    Args:
        s3_path (str): S3 URI (e.g., 's3://bucket/key')

    Returns:
        tuple: (bucket, key)
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"Invalid S3 path: {s3_path}")
    path = s3_path[5:]
    bucket, key = path.split("/", 1)
    return bucket, key

def get_all_trigger_files(bucket: str, prefix: str) -> list:
    """
    Lists all Step Function trigger JSON files in the specified S3 bucket and prefix.

    Args:
        bucket (str): S3 bucket name
        prefix (str): S3 prefix to search under

    Returns:
        list: List of full S3 URIs to trigger files
    """
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
    """
    Loads the validated files state from S3.

    Returns:
        set: Set of validated file S3 paths
    """
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

def get_transformed_files() -> set:
    """
    Loads the transformed files state from S3.

    Returns:
        set: Set of transformed file S3 paths
    """
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
    """
    Updates the transformed files state file in S3.

    Args:
        files_set (set): Set of transformed file S3 paths
    """
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
    """
    Loads a JSON file from S3.

    Args:
        s3_path (str): Full S3 URI to the JSON file

    Returns:
        dict: Parsed JSON content
    """
    bucket, key = parse_s3_path(s3_path)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        return json.loads(content)
    except Exception as e:
        log_and_buffer("error", f"Failed to load JSON from {s3_path}: {e}")
        raise

def main():
    """
    Main entry point for the transformation job.
    Loads trigger files, filters files to transform, performs transformations,
    updates state, and logs all actions.
    """
    log_and_buffer("info", "Starting transformation job")
    batch_id = f"batch_{uuid4()}"

    # Initialize Spark session with S3A support and AWS credentials provider
    try:
        spark = SparkSession.builder.appName("ECS-Transformation") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.eu-north-1.amazonaws.com") \
            .getOrCreate()
        log_and_buffer("info", "Spark session started.")
    except Exception as e:
        log_and_buffer("error", f"Failed to start Spark session: {e}")
        upload_logs_to_s3(batch_id)
        return

    # Load validated and transformed files state
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

    # Process each trigger file
    for trigger_file_path in trigger_files:
        try:
            trigger_data = load_json_from_s3(trigger_file_path)
            log_and_buffer("info", f"Loaded trigger file from {trigger_file_path}")
            groups = trigger_data.get("groups", [])
        except Exception as e:
            log_and_buffer("error", f"Failed to load/parse trigger file {trigger_file_path}: {e}")
            continue

        # Process each group and its files
        for group in groups:
            for file_info in group.get("files", []):
                s3_path = file_info.get("path")
                file_type = file_info.get("type")
                if not s3_path or not file_type:
                    log_and_buffer("warning", f"Skipping invalid file info: {file_info}")
                    continue
                if s3_path not in files_to_transform:
                    continue  # Skip files already transformed or not validated

                try:
                    # Replace s3:// with s3a:// for Spark compatibility
                    spark_s3_path = s3_path.replace("s3://", "s3a://")

                    log_and_buffer("info", f"Transforming file: {spark_s3_path} (type: {file_type})")

                    # Read Parquet file
                    df = spark.read.parquet(spark_s3_path)

                    # TODO: Add your transformation logic here
                    # For demonstration, write to a 'transformed/' prefix in the same bucket
                    transformed_key = s3_path.replace("/raw/", "/transformed/")
                    if transformed_key == s3_path:
                        transformed_key = s3_path.replace("/state/", "/transformed/")

                    output_path = f"s3a://{PROJECT_BUCKET}/" + transformed_key.split(f"{PROJECT_BUCKET}/")[-1]

                    # Write transformed data
                    df.write.mode("overwrite").parquet(output_path)

                    log_and_buffer("info", f"Transformed and wrote to {output_path}")

                    # Update transformed files state
                    transformed_files.add(s3_path)
                    update_transformed_files(transformed_files)

                except Exception as e:
                    log_and_buffer("error", f"Failed to transform file {s3_path}: {e}")

    # Upload logs to S3 after processing
    upload_logs_to_s3(batch_id)

if __name__ == "__main__":
    main()
