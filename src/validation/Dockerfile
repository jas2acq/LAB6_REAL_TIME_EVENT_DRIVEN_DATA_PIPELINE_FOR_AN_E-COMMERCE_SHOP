# Use official Python slim image for smaller footprint
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY validate.py .

# Command to run the validation service
CMD ["python", "validate.py"]