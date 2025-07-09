# -----------------------------------------------------------
# PySpark ECS Transformation Dockerfile
# -----------------------------------------------------------
# - Uses official Spark image for full Spark support
# - Installs boto3 for DynamoDB and S3 access
# - Adds Hadoop AWS JARs for S3 connectivity
# - Copies your transformation script and requirements
# - Entry point uses spark-submit for PySpark
# -----------------------------------------------------------

# Use official Spark 3.4.1 image with Hadoop 3.3

FROM apache/spark-py:v3.4.1-hadoop3


# Add required Hadoop AWS JARs for S3 access
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /usr/local/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.698/aws-java-sdk-bundle-1.12.698.jar /usr/local/spark/jars/

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY transform.py .

# Default command to run your transformation (argument: trigger file S3 path)
ENTRYPOINT ["spark-submit", "transform.py"]