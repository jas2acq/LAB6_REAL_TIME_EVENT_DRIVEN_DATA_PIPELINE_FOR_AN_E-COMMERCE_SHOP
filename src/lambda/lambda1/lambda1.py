import json
import boto3
import os
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
glue_client = boto3.client('glue')

# Environment variables
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME', 'lab6-glue-job')
PROJECT_BUCKET = os.environ.get('PROJECT_BUCKET', 'lab-6-project')

def lambda_handler(event, context):
    """
    Lambda handler that triggers the Glue Python Shell job,
    passing the incoming event as a JSON string argument.
    """
    logger.info(f"Lambda triggered with event: {json.dumps(event)}")

    try:
        # Serialize event as JSON string to pass as Glue job argument
        event_str = json.dumps(event)

        # Start Glue job with event JSON as argument
        response = glue_client.start_job_run(
            JobName=GLUE_JOB_NAME,
            Arguments={
                '--event_json': event_str
            }
        )
        job_run_id = response['JobRunId']
        logger.info(f"Started Glue job '{GLUE_JOB_NAME}' with JobRunId: {job_run_id}")

        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Glue job started successfully',
                'jobRunId': job_run_id
            })
        }

    except Exception as e:
        logger.error(f"Failed to start Glue job: {e}", exc_info=True)
        raise e

print("test")