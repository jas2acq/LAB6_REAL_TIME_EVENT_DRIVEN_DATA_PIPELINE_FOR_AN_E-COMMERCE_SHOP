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
from pyspark.sql.functions import col
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

s3_log_buffer = []

def log_and_buffer(level, message):
    getattr(logger, level)(message)
    s3_log_buffer.append(f"[{datetime.now().isoformat()}] [{level.upper()}] {message}")

def upload_logs_to_s3(batch_id):
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

def parse_s3_path(s3_path):
    assert s3_path.startswith("s3://")
    path = s3_path[5:]
    bucket, key = path.split("/", 1)
    return bucket, key

def get_all_trigger_files(bucket, prefix):
    trigger_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json') and TRIGGER_FILE_PREFIX in key:
                trigger_files.append(f"s3://{bucket}/{key}")
    return trigger_files

def get_validated_files():
    try:
        obj = s3_client.get_object(Bucket=PROJECT_BUCKET, Key=VALIDATED_STATE_KEY)
        state = json.loads(obj["Body"].read())
        return set(state.get("validated_files", []))
    except s3_client.exceptions.NoSuchKey:
        log_and_buffer("warning", f"No validated state file found, assuming nothing validated yet.")
        return set()

def get_transformed_files():
    try:
        obj = s3_client.get_object(Bucket=PROJECT_BUCKET, Key=TRANSFORMED_STATE_KEY)
        state = json.loads(obj["Body"].read())
        return set(state.get("transformed_files", []))
    except s3_client.exceptions.NoSuchKey:
        log_and_buffer("info", f"No transformed state file found, starting fresh.")
        return set()

def update_transformed_files(files_set):
    s3_client.put_object(
        Bucket=PROJECT_BUCKET,
        Key=TRANSFORMED_STATE_KEY,
        Body=json.dumps({"transformed_files": list(files_set)}, indent=2).encode("utf-8")
    )
    log_and_buffer("info", f"Updated transformed state file with {len(files_set)} files.")

def load_json_from_s3(s3_path):
    bucket, key = parse_s3_path(s3_path)
    response = s3_client.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')
    return json.loads(content)

def main():
    log_and_buffer("info", "Starting transformation job")
    batch_id = f"batch_{uuid4()}"
    try:
        spark = SparkSession.builder.appName("ECS-Transformation").getOrCreate()
        log_and_buffer("info", "Spark session started.")
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

    # Find all trigger files (batches)
    trigger_files = get_all_trigger_files(PROJECT_BUCKET, STATE_DATA_PREFIX + TRIGGER_FILE_PREFIX)
    if not trigger_files:
        log_and_buffer("warning", "No trigger files found to process.")
        upload_logs_to_s3(batch_id)
        return

    for trigger_file_path in trigger_files:
        try:
            trigger_data = load_json_from_s3(trigger_file_path)
            log_and_buffer("info", f"Loaded trigger file from {trigger_file_path}")
            groups = trigger_data.get("groups", [])
        except Exception as e:
            log_and_buffer("error", f"Failed to load/parse trigger file {trigger_file_path}: {e}")
            continue

        for group in groups:
            for file_info in group.get("files", []):
                s3_path = file_info.get("path")
                file_type = file_info.get("type")
                if not s3_path or not file_type:
                    log_and_buffer("warning", f"Skipping invalid file info: {file_info}")
                    continue
                if s3_path not in files_to_transform:
                    continue  # Only transform validated and not-yet-transformed files

                try:
                    # Actual transformation logic goes here
                    log_and_buffer("info", f"Transforming file: {s3_path} (type: {file_type})")
                    # Example: read Parquet, do a trivial transformation, write to another location
                    df = spark.read.parquet(s3_path)
                    # (Add your transformation logic here, e.g., aggregations, joins, etc.)
                    # For demo, write to a transformed/ prefix in the same bucket
                    transformed_key = s3_path.replace("/raw/", "/transformed/")
                    if transformed_key == s3_path:
                        # fallback if not using /raw/
                        transformed_key = s3_path.replace("/state/", "/transformed/")
                    output_path = f"s3://{PROJECT_BUCKET}/" + transformed_key.split(f"{PROJECT_BUCKET}/")[-1]
                    df.write.mode("overwrite").parquet(output_path)
                    log_and_buffer("info", f"Transformed and wrote to {output_path}")
                    transformed_files.add(s3_path)
                    update_transformed_files(transformed_files)
                except Exception as e:
                    log_and_buffer("error", f"Failed to transform file {s3_path}: {e}")

    upload_logs_to_s3(batch_id)

if __name__ == "__main__":
    main()
