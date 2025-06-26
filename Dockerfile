FROM python:3.9-slim


WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# Copy application code
COPY . .

RUN mkdir -p data/raw data/processed data/logs

# Set Python path
ENV PYTHONPATH=/app

# Default command
CMD ["python", "src/etl/pipeline.py"]