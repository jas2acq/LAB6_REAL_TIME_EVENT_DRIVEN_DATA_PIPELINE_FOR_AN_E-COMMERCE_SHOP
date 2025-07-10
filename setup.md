# Setup Guide

## Prerequisites
- AWS account with appropriate IAM permissions.
- Docker installed for building container images.
- AWS CLI configured with credentials.
- GitHub repository with secrets configured (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `PROJECT_BUCKET`, `GLUE_SCRIPT_S3_PATH`, `LAMBDA1_FUNCTION_NAME`, `LAMBDA2_FUNCTION_NAME`, `SQS_QUEUE_URL`, `STEP_FUNCTIONS_ARN`).
- SQS queue configured with appropriate permissions.

## Installation Steps
1. **Clone the Repository**:
   ```bash
   git clone <repository-url>
   cd LAB6_REAL_TIME_EVENT_DRIVEN_DATA_PIPE...
   ```

2. **Build Containers**:
   - Navigate to `containers/transformation` and run:
     ```bash
     docker build -t transformation-image .
     ```
   - Navigate to `containers/validation` and run:
     ```bash
     docker build -t validation-image .
     ```

3. **Push Images to ECR**:
   - Push container images to ECR by committing changes to the `main` branch or using the `workflow_dispatch` trigger with `deploy_to_ECR.yaml`.

4. **Configure AWS Services**:
   - Set up S3 bucket (`lab-6-project`) for input files, state, logs, and Delta Lake tables.
   - Create DynamoDB tables (`ecom_category_kpis`, `ecom_order_kpis_daily`) as per the schema.
   - Deploy Step Functions workflow using the provided YAML files, ensuring `STEP_FUNCTIONS_ARN` is set.
   - Configure Glue job by pushing changes to the `main` branch or using the `workflow_dispatch` trigger, ensuring SQS integration.
   - Configure Lambda functions by pushing changes to the `main` branch or using the `workflow_dispatch` trigger, setting `LAMBDA1_FUNCTION_NAME` and `LAMBDA2_FUNCTION_NAME`.

5. **Run the Pipeline**:
   - Upload a sample CSV file to the S3 bucket to trigger the Glue job via SQS.
   - Monitor execution via AWS Console.

## Troubleshooting
- Check CloudWatch Logs for ECS task, Glue job, or Lambda function errors.
- Ensure IAM roles have necessary permissions for S3, DynamoDB, SQS, Step Functions, and Glue.
- Verify GitHub secrets and S3/SQS paths in the `deploy_glue_scripts.yaml`, `deploy_lambda_functions.yaml`, and `deploy_to_ECR.yaml` workflows.