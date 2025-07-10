# Real-Time Event-Driven Data Pipeline for an E-Commerce Shop

## Overview
This project implements a real-time, event-driven data pipeline for an e-commerce platform using AWS-native services. It processes transactional data (orders, products) arriving as flat files in an Amazon S3 bucket, validates and transforms the data into business KPIs, and stores the results in Amazon DynamoDB for real-time querying.

## Architecture
![Architecture Diagram](imgs/lab6_architecture.svg)

The architecture follows a flowchart schema representing an AWS cloud-based network pipeline. It includes:
- **Data Source**: User-uploaded flat files to an S3 bucket.
- **Trigger Event**: S3 event triggers a Glue Shell script.
- **Orchestration**: AWS Step Functions manage the workflow, coordinating ECS tasks for validation and transformation.
- **Validation**: ECS task validates data using a lightweight Python container, checking schema, required fields, and business logic.
- **Transformation**: ECS task transforms validated Parquet files using Spark and Delta Lake, computing KPIs.
- **Storage**: KPIs are stored in DynamoDB tables and Delta Lake files.
- **Logging**: CloudWatch and S3 logs track process execution and errors.
- **State Machine**: Step Functions maintain state with S3 state files.
- **Producer/Consumer**: Lambda functions and Glue jobs act as producers, triggering downstream consumer tasks.
- **VPC**: ECS tasks operate within a Virtual Private Cloud for security.
- **Referential Integrity**: Ensured during Glue job group identification.
- **Dynamite Table**: Refers to DynamoDB tables for KPI storage.

## Project Structure
```
LAB6_REAL_TIME_EVENT_DRIVEN_DATA_PIPE...
├── github/workflows
│   ├── deploy_glue_scripts.yaml
│   ├── deploy_lambda_functions.yaml
│   └── deploy_to_ECR.yaml
├── containers
│   ├── transformation
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   └── transform.py
│   └── validation
│       ├── Dockerfile
│       ├── requirements.txt
│       └── validate.py
├── imgs
│   ├── lab6_architecture.png
│   └── lab6_architecture.svg
├── src
│   ├── glue_scripts
│   │   └── glue_job.py
│   └── lambda
│       ├── lambda1
│       │   ├── lambda1.py
│       │   └── requirements.txt
│       └── lambda2
│           ├── lambda2.py
│           └── requirements.txt
├── .gitignore
├── README.md
└── setup.md
```

## Data Format and Sample Schema
- **Input Format**: Flat files stored in Amazon S3, converted to Parquet (orders, order_items, products).
- **Sample Schema**: 
  - `orders`: `order_id` (string), `user_id` (string), `status` (string), `created_at` (timestamp), `returned_at` (timestamp), `shipped_at` (timestamp), `delivered_at` (timestamp), `num_of_item` (integer)
  - `order_items`: `id` (string), `order_id` (string), `user_id` (string), `product_id` (string), `status` (string), `created_at` (timestamp), `shipped_at` (timestamp), `delivered_at` (timestamp), `returned_at` (timestamp), `sale_price` (float)
  - `products`: `id` (string), `sku` (string), `cost` (float), `category` (string), `name` (string), `brand` (string), `retail_price` (float), `department` (string)

## Validation Rules
- **Schema Validation**: Ensures all expected columns are present and no duplicates exist.
  - `orders`: `order_id`, `user_id`, `status`, `created_at`, `returned_at`, `shipped_at`, `delivered_at`, `num_of_item`
  - `order_items`: `id`, `order_id`, `user_id`, `product_id`, `status`, `created_at`, `shipped_at`, `delivered_at`, `returned_at`, `sale_price`
  - `products`: `id`, `sku`, `cost`, `category`, `name`, `brand`, `retail_price`, `department`
- **Required Fields**: Checks for null values in required columns:
  - `orders`: `order_id`, `user_id`, `status`, `created_at`
  - `order_items`: `id`, `order_id`, `user_id`, `product_id`, `status`
  - `products`: `id`, `sku`, `name`, `brand`
- **Unique Key Constraints**: Ensures uniqueness of:
  - `orders`: `order_id`
  - `order_items`: `order_id`, `product_id`
  - `products`: `id`
- **Business Logic**:
  - `orders`: `status` must be in `["pending", "processing", "shipped", "delivered", "cancelled", "returned"]`
  - `order_items`: `sale_price` must be non-negative
  - `products`: `cost` and `retail_price` must be non-negative
  - `orders`: `num_of_item` must be an integer
- Exit pipeline gracefully on validation failure with retry mechanism (up to 3 attempts).

## DynamoDB Schema
### Category-Level KPIs Table
- **Partition Key**: `category`
- **Sort Key**: `order_date`
- **Attributes**: 
  - `daily_revenue` (float)
  - `avg_order_value` (float)
  - `avg_return_rate` (float)

### Order-Level KPIs Table
- **Partition Key**: `order_date`
- **Attributes**: 
  - `total_orders` (integer)
  - `total_revenue` (float)
  - `total_items_sold` (integer)
  - `return_rate` (float)
  - `unique_customers` (integer)

## Step Function Workflow
- **Start**: Triggered by S3 event via Lambda 2, initiating Step Functions.
- **Validate**: ECS task to validate incoming files using a lightweight Python container, performing detailed file-level validation.
- **Transform**: ECS task to compute KPIs from validated data using Spark and Delta Lake.
- **Store**: Write KPIs to DynamoDB.
- **Archive**: Log processed files and outcomes.
- **Error Handling**: Branch to failure state on validation or transformation errors.

## Transformation Logic
The transformation task (`transform.py`) uses Apache Spark with Delta Lake for scalable data processing:
- **Input**: Loads validated Parquet files from S3 (orders, order_items, products) based on Step Function trigger files.
- **Processing**: 
  - Joins datasets and computes KPIs:
    - **Category-Level KPIs**: `daily_revenue`, `avg_order_value`, `avg_return_rate`.
    - **Order-Level KPIs**: `total_orders`, `total_revenue`, `total_items_sold`, `return_rate`, `unique_customers`.
  - Uses Delta Lake for upsert semantics to maintain updated KPIs.
- **Output**: 
  - Stores KPIs in Delta Lake tables (`category_kpis`, `order_kpis_daily`).
  - Pushes KPIs to DynamoDB with batch writes and retries.
- **State Management**: Tracks validated and transformed files in S3 state files.
- **Logging**: Writes logs to both stdout and S3.

## Glue Job Logic
The Glue job (`glue_job.py`) acts as the initial trigger:
- **Input**: Processes SQS messages containing S3 event notifications.
- **Processing**: 
  - Converts CSV files to Parquet and uploads to S3.
  - Performs initial schema validation against expected schemas.
  - Extracts and tracks order/product IDs in a manifest file.
  - Ensures referential integrity by identifying complete groups (all related `order_ids` and `product_ids`) and generates Step Function trigger files.
- **Output**: Updates a manifest in S3 and triggers downstream validation.

## Lambda Functions
- **Lambda 1 (`lambda1.py`)**: 
  - **Purpose**: Triggers the Glue job (`lab6-glue-job`) with the incoming event as a JSON argument.
  - **Input**: AWS event (e.g., S3 notification).
  - **Output**: Initiates Glue job and returns job run ID.
- **Lambda 2 (`lambda2.py`)**: 
  - **Purpose**: Monitors S3 for trigger files and initiates Step Function executions.
  - **Input**: S3 event notifications for `state/step_function_trigger_*.json` files.
  - **Output**: Starts Step Function execution with trigger data and logs to S3.

## Error Handling, Retry, and Logging
- **Logging**: Use CloudWatch Logs for ECS task execution, Glue jobs, and Lambda functions; S3 for detailed logs.
- **Error Handling**: Step Functions include failure paths with timeouts; validation includes retries (up to 3 attempts); Glue and Lambda log errors without halting.
- **Retry**: Up to 3 retries for unprocessed DynamoDB items in transformation.

## Simulation Steps
1. Upload a sample CSV file to the S3 bucket to trigger the Glue job via SQS.
2. Verify Lambda 1 triggers the Glue job and Lambda 2 initiates Step Functions via CloudWatch logs.
3. Check validation and transformation logs in CloudWatch.
4. Verify KPI data in DynamoDB tables.
5. Review archived files and Delta Lake tables in S3.

## Deployment
- **Glue Jobs**: Automated deployment of Glue scripts via `github/workflows/deploy_glue_scripts.yaml`. The workflow uploads `glue_job.py` to an S3 bucket and manages the Glue job (`lab6-glue-job`) creation or updates.
- **Lambda Functions**: Automated deployment of Lambda functions via `github/workflows/deploy_lambda_functions.yaml`. The workflow packages and uploads `lambda1` and `lambda2` to S3, then updates their code and configurations in AWS Lambda.
- **ECR Images**: Automated build and push of container images (`lab6-validation` and `lab6-transformation`) to Amazon ECR via `github/workflows/deploy_to_ECR.yaml`.