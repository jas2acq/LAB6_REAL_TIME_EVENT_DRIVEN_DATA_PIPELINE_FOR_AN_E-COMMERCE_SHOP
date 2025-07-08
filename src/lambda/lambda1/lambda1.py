import json
import boto3
import os
import io
import pandas as pd
from datetime import datetime
import uuid
from botocore.exceptions import ClientError

# --- AWS Clients ---
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# --- Environment Variables ---
PROJECT_BUCKET = os.environ.get('PROJECT_BUCKET', 'lab-6-project')
RAW_DATA_PREFIX = os.environ.get('RAW_DATA_PREFIX', 'raw/')
STATE_DATA_PREFIX = os.environ.get('STATE_DATA_PREFIX', 'state/')
LOGS_DATA_PREFIX = os.environ.get('LOGS_DATA_PREFIX', 'logs/')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL', 'YOUR_SQS_QUEUE_URL')

# --- Schemas aligned with your real sample data ---
EXPECTED_SCHEMAS = {
    "orders": ["order_id", "user_id", "status", "created_at", "returned_at", "shipped_at", "delivered_at", "num_of_item"],
    "order_items": ["id", "order_id", "user_id", "product_id", "status", "created_at", "shipped_at", "delivered_at", "returned_at", "sale_price"],
    "products": ["id", "sku", "cost", "category", "name", "brand", "retail_price", "department"]
}

UNIQUE_KEYS = {
    "orders": "order_id",
    "order_items": ["order_id", "product_id"],
    "products": "id"  # product_id is stored as 'id' in products.csv
}

PENDING_MANIFEST_KEY = f"{STATE_DATA_PREFIX}pending_grouping_manifest.json"
TRIGGER_FILE_PREFIX = f"{STATE_DATA_PREFIX}step_function_trigger_"

# --- In-memory log buffer ---
s3_log_buffer = []

def custom_logger(message, level="INFO"):
    timestamp = datetime.now().isoformat()
    log_entry = f"[{timestamp}] [{level}] {message}"
    print(log_entry)
    s3_log_buffer.append(log_entry)

def write_s3_logs(context):
    if not s3_log_buffer:
        return
    log_file_key = f"{LOGS_DATA_PREFIX}lambda1_logs/{datetime.now().strftime('%Y/%m/%d')}/{context.aws_request_id}-{uuid.uuid4()}.log"
    try:
        s3_client.put_object(
            Bucket=PROJECT_BUCKET,
            Key=log_file_key,
            Body="\n".join(s3_log_buffer).encode('utf-8')
        )
        custom_logger(f"S3 log written to s3://{PROJECT_BUCKET}/{log_file_key}")
    except Exception as e:
        custom_logger(f"Failed to write logs: {e}", level="ERROR")

def infer_file_type(s3_key):
    if RAW_DATA_PREFIX not in s3_key:
        if "orders/" in s3_key:
            return "orders"
        elif "order_items/" in s3_key:
            return "order_items"
        elif "products/" in s3_key:
            return "products"
    return None

def get_manifest():
    try:
        response = s3_client.get_object(Bucket=PROJECT_BUCKET, Key=PENDING_MANIFEST_KEY)
        return json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            custom_logger("Manifest not found. Initializing new one.")
            return {
                "processed_files": {},
                "pending_order_ids_from_orders": [],
                "pending_order_ids_from_order_items": [],
                "pending_product_ids_from_products": [],
                "pending_product_ids_from_order_items": [],
                "order_items_product_map": {},
                "completed_order_ids": []
            }
        raise

def put_manifest(manifest_data):
    s3_client.put_object(
        Bucket=PROJECT_BUCKET,
        Key=PENDING_MANIFEST_KEY,
        Body=json.dumps(manifest_data, indent=2)
    )
    custom_logger("Manifest updated in S3.")

def extract_ids_from_df(df, file_type):
    """
    Extracts and standardizes order/product IDs from the DataFrame.
    """
    extracted = {}
    if file_type == "orders":
        extracted['order_ids'] = df[UNIQUE_KEYS['orders']].dropna().astype(str).drop_duplicates().tolist()
    elif file_type == "order_items":
        df[UNIQUE_KEYS['order_items'][0]] = df[UNIQUE_KEYS['order_items'][0]].astype(str)
        df[UNIQUE_KEYS['order_items'][1]] = df[UNIQUE_KEYS['order_items'][1]].astype(str)
        extracted['order_ids'] = df[UNIQUE_KEYS['order_items'][0]].dropna().drop_duplicates().tolist()
        extracted['product_ids'] = df[UNIQUE_KEYS['order_items'][1]].dropna().drop_duplicates().tolist()
        extracted['order_product_mapping'] = df.groupby(UNIQUE_KEYS['order_items'][0])[UNIQUE_KEYS['order_items'][1]].apply(lambda x: x.drop_duplicates().tolist()).to_dict()
    elif file_type == "products":
        extracted['product_ids'] = df[UNIQUE_KEYS['products']].dropna().astype(str).drop_duplicates().tolist()
    return extracted

def lambda_handler(event, context):
    custom_logger("Lambda 1 invoked.")
    s3_log_buffer.clear()

    for record in event['Records']:
        message = json.loads(record['body'])
        receipt_handle = record['receiptHandle']

        if 'Records' not in message:
            continue

        s3_event = message['Records'][0]
        bucket = s3_event['s3']['bucket']['name']
        key = s3_event['s3']['object']['key']
        custom_logger(f"Processing file: {key}")

        try:
            file_type = infer_file_type(key)
            if not file_type:
                custom_logger(f"Unrecognized file type. Skipping: {key}", level="WARN")
                continue

            response = s3_client.get_object(Bucket=bucket, Key=key)
            df = pd.read_csv(io.BytesIO(response['Body'].read()))

            if not all(col in df.columns for col in EXPECTED_SCHEMAS[file_type]):
                custom_logger(f"Schema mismatch in {key}. Found columns: {df.columns.tolist()}", level="ERROR")
                continue

            parquet_key = f"{RAW_DATA_PREFIX}{file_type}/{os.path.basename(key).replace('.csv', '.parquet')}"
            df.to_parquet(io_buffer := io.BytesIO(), index=False, compression='snappy')
            s3_client.put_object(Bucket=PROJECT_BUCKET, Key=parquet_key, Body=io_buffer.getvalue())
            custom_logger(f"Converted and uploaded Parquet: {parquet_key}")

            ids = extract_ids_from_df(df, file_type)
            manifest = get_manifest()
            manifest['processed_files'][f"s3://{PROJECT_BUCKET}/{parquet_key}"] = {
                "file_type": file_type,
                "order_ids": ids.get("order_ids", []),
                "product_ids": ids.get("product_ids", []),
                "order_product_mapping": ids.get("order_product_mapping", {})
            }

            if 'order_ids' in ids:
                key_name = 'pending_order_ids_from_orders' if file_type == "orders" else 'pending_order_ids_from_order_items'
                manifest[key_name].extend(ids['order_ids'])

            if 'product_ids' in ids:
                key_name = 'pending_product_ids_from_products' if file_type == "products" else 'pending_product_ids_from_order_items'
                manifest[key_name].extend(ids['product_ids'])

            if 'order_product_mapping' in ids:
                for oid, pids in ids['order_product_mapping'].items():
                    if oid not in manifest['order_items_product_map']:
                        manifest['order_items_product_map'][oid] = []
                    manifest['order_items_product_map'][oid].extend(pids)
                    manifest['order_items_product_map'][oid] = list(set(manifest['order_items_product_map'][oid]))

            for k in ['pending_order_ids_from_orders', 'pending_order_ids_from_order_items',
                      'pending_product_ids_from_products', 'pending_product_ids_from_order_items']:
                manifest[k] = list(set(manifest[k]))

            put_manifest(manifest)

            # Delete message only after successful processing
            sqs_client.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            custom_logger(f"Deleted SQS message for: {key}")

        except Exception as e:
            custom_logger(f"Error processing file {key}: {e}", level="ERROR")
            continue

    # Grouping logic to identify referentially complete groups
    manifest = get_manifest()
    candidate_orders = set(manifest['pending_order_ids_from_order_items']) - set(manifest['completed_order_ids'])
    completed_groups = []
    newly_completed = set()

    for oid in candidate_orders:
        if oid not in manifest['pending_order_ids_from_orders']:
            continue
        product_ids = manifest['order_items_product_map'].get(str(oid), [])
        if not all(pid in manifest['pending_product_ids_from_products'] for pid in product_ids):
            continue

        group_files = []
        for path, meta in manifest['processed_files'].items():
            if meta['file_type'] == 'orders' and oid in meta['order_ids']:
                group_files.append({"path": path, "type": "orders"})
            elif meta['file_type'] == 'order_items' and oid in meta['order_ids']:
                group_files.append({"path": path, "type": "order_items"})
            elif meta['file_type'] == 'products' and any(pid in meta['product_ids'] for pid in product_ids):
                group_files.append({"path": path, "type": "products"})

        seen = set()
        unique_group_files = [f for f in group_files if f['path'] not in seen and not seen.add(f['path'])]

        completed_groups.append({
            "order_id": oid,
            "files": unique_group_files
        })
        newly_completed.add(oid)

    if completed_groups:
        custom_logger(f"{len(completed_groups)} group(s) completed.")
        manifest['completed_order_ids'].extend(list(newly_completed))
        manifest['completed_order_ids'] = list(set(manifest['completed_order_ids']))
        put_manifest(manifest)

        trigger_key = f"{TRIGGER_FILE_PREFIX}{uuid.uuid4()}.json"
        trigger_data = {
            "batch_id": context.aws_request_id,
            "timestamp": datetime.now().isoformat(),
            "groups": completed_groups
        }
        s3_client.put_object(
            Bucket=PROJECT_BUCKET,
            Key=trigger_key,
            Body=json.dumps(trigger_data, indent=2)
        )
        custom_logger(f"Trigger file created: {trigger_key}")
    else:
        custom_logger("No complete groups found.")

    write_s3_logs(context)

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda 1 execution complete.')
    }


print("test")