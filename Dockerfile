# Base Image: Lightweight Python environment
FROM python:3.9-slim

# Set working directory inside the container
WORKDIR /app

# Install dependencies
# We use 'kafka-python-ng' to ensure compatibility with modern Python versions
RUN pip install --no-cache-dir kafka-python-ng pyspark

# Copy source code
# While the Volume in docker-compose handles live sync, 
# this COPY instruction is standard practice for building the image artifact.
COPY . .

# Default command
# (Note: This is overridden by 'command: tail -f /dev/null' in docker-compose for dev mode)
CMD ["python", "-u", "weather_producer.py"]