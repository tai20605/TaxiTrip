FROM python:3.10-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir confluent-kafka==2.3.0

# Copy the producer script
COPY producer.py /app/producer.py

# Run Python with unbuffered output to ensure immediate logs in Docker
CMD ["python", "-u", "/app/producer.py"]
