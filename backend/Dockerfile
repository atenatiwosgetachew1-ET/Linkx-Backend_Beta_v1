FROM python:3.11-slim

WORKDIR /app

# Install Java for PySpark if needed
RUN apt-get update && \
    apt-get install -y default-jdk && \
    rm -rf /var/lib/apt/lists/*

# Install core Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app code (you can also skip this in dev and mount as volume)
COPY . .

# Default command
CMD ["python", "main.py"]
