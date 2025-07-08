import json
import boto3
import os
import uuid
from datetime import datetime
from botocore.exceptions import ClientError

# --- AWS Clients ---
s3_client = boto3.client('s3')
stepfunctions_client = boto3.client('stepfunctions')

# --- Environment Variables (all required) ---
PROJECT_BUCKET = os.environ['PROJECT_BUCKET']
STEP_FUNCTIONS_ARN = os.environ['STEP_FUNCTIONS_ARN']
LOGS_DATA_PREFIX = os.environ.get('LOGS_DATA_PREFIX', 'logs/')

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
    log_file_key = f"{LOGS_DATA_PREFIX}lambda2_logs/{datetime.now().strftime('%Y/%m/%d')}/{context.aws_request_id}-{uuid.uuid4()}.log"
    try:
        s3_client.put_object(
            Bucket=PROJECT_BUCKET,
            Key=log_file_key,
            Body="\n".join(s3_log_buffer).encode('utf-8')
        )
        custom_logger(f"S3 log written to s3://{PROJECT_BUCKET}/{log_file_key}")
    except Exception as e:
        custom_logger(f"Failed to write logs: {e}", level="ERROR")

def lambda_handler(event, context):
    s3_log_buffer.clear()
    custom_logger("Lambda 2 invoked.")

    for record in event['Records']:
        s3_info = record['s3']
        bucket = s3_info['bucket']['name']
        key = s3_info['object']['key']
        custom_logger(f"Received S3 event for: {key}")

        # Only process trigger files
        if not key.startswith('state/step_function_trigger_') or not key.endswith('.json'):
            custom_logger(f"Skipped non-trigger file: {key}", level="WARNING")
            continue

        try:
            # Read trigger file from S3
            response = s3_client.get_object(Bucket=bucket, Key=key)
            trigger_data = json.loads(response['Body'].read().decode('utf-8'))
            custom_logger(f"Loaded trigger data: {trigger_data}")

            # Start Step Function execution
            execution_name = f"exec-{datetime.now().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
            step_fn_response = stepfunctions_client.start_execution(
                stateMachineArn=STEP_FUNCTIONS_ARN,
                name=execution_name,
                input=json.dumps(trigger_data)
            )
            custom_logger(f"Started Step Function execution: {step_fn_response['executionArn']}")

        except ClientError as ce:
            custom_logger(f"AWS ClientError: {ce}", level="ERROR")
        except Exception as e:
            custom_logger(f"Error processing trigger file {key}: {e}", level="ERROR")

    write_s3_logs(context)

    return {
        'statusCode': 200,
        'body': json.dumps('Lambda 2 execution complete.')
    }
