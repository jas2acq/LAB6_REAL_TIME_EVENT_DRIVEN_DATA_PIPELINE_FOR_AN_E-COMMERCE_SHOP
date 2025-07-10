FROM apache/spark-py

USER root

# Add Hadoop AWS and AWS SDK JARs for S3 support (compatible versions)
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/

# Add Delta Lake JARs (version 2.4.0 to match requirements.txt)
ADD https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar /opt/spark/jars/

# Add additional required JARs for Delta Lake storage
ADD https://repo1.maven.org/maven2/org/antlr/antlr4-runtime/4.8/antlr4-runtime-4.8.jar /opt/spark/jars/

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY transform.py .

# Default command to run your transformation (argument: trigger file S3 path)
ENTRYPOINT ["spark-submit", "transform.py"]