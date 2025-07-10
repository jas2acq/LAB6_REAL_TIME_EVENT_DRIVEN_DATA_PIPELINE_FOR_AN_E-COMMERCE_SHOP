Setup Guide
Prerequisites

AWS account with appropriate IAM permissions.
Docker installed for building container images.
AWS CLI configured with credentials.
GitHub repository with secrets configured (AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, PROJECT_BUCKET, GLUE_SCRIPT_S3_PATH, LAMBDA1_FUNCTION_NAME, LAMBDA2_FUNCTION_NAME, SQS_QUEUE_URL, STEP_FUNCTIONS_ARN, SUBNET_1, SUBNET_2, SUBNET_3, SECURITY_GROUP).
SQS queue configured with appropriate permissions.
SNS topic (arn:aws:sns:eu-north-1:476114131873:Lab_5) configured for notifications.
ECS cluster (lab6-ecs-cluster) and task definitions (lab6-task-validate, lab6-task-transform) set up with appropriate IAM roles.

Installation Steps

Clone the Repository:
git clone <repository-url>
cd LAB6_REAL_TIME_EVENT_DRIVEN_DATA_PIPE...


Build Containers:

Navigate to containers/transformation and run:docker build -t transformation-image .


Navigate to containers/validation and run:docker build -t validation-image .




Push Images to ECR:

Push container images to ECR by committing changes to the main branch or using the workflow_dispatch trigger with deploy_to_ECR.yaml.


Configure AWS Services:

Set up S3 bucket (lab-6-project) for input files, state, logs, and Delta Lake tables.
Create DynamoDB tables (ecom_category_kpis, ecom_order_kpis_daily) as per the schema (optional, as transformation will create them if missing).
Deploy Step Functions workflow using the provided YAML files and deploy_step_functions.yaml, ensuring STEP_FUNCTIONS_ARN is set and subnet/security group secrets are configured.
Configure Glue job by pushing changes to the main branch or using the workflow_dispatch trigger, ensuring SQS integration.
Configure Lambda functions by pushing changes to the main branch or using the workflow_dispatch trigger, setting LAMBDA1_FUNCTION_NAME and LAMBDA2_FUNCTION_NAME.
Set up ECS cluster and task definitions with Fargate launch type, linking to ECR images.


Run the Pipeline:

Upload a sample CSV file to the S3 bucket to trigger the Glue job via SQS.
Monitor execution via AWS Console and SNS notifications.



Troubleshooting

Check CloudWatch Logs for ECS task, Glue job, or Lambda function errors.
Ensure IAM roles have necessary permissions for S3, DynamoDB, SQS, Step Functions, Glue, ECS, SNS, and Spark.
Verify GitHub secrets and S3/SQS paths in the deploy_glue_scripts.yaml, deploy_lambda_functions.yaml, deploy_step_functions.yaml, and deploy_to_ECR.yaml workflows.
Validate Step Functions deployment status using aws stepfunctions describe-state-machine.
