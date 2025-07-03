# Use official Python slim image for smaller footprint
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY transformation.py .

# Command to run the transformation service
CMD ["python", "transformation.py"]