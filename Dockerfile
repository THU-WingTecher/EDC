FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create necessary directories with proper permissions
RUN mkdir -p /app/log /app/res /app/db && \
    chmod -R 777 /app/log /app/res /app/db

# Copy src directory first to ensure seed directory exists
COPY src /app/src

# Copy the rest of the application
COPY . .

# Set environment variables
ENV PYTHONPATH=/app

# Use bash as the default command
CMD ["/bin/bash"] 