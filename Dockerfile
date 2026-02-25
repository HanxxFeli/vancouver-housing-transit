# Start from the official Python 3.11 image
FROM python:3.11-slim

# Install Java — PySpark requires Java to run
RUN apt-get update && apt-get install -y \
    default-jre \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so PySpark can find Java
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Set working directory inside the container
WORKDIR /app

# Copy requirements first — Docker caches layers
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the source code
COPY . .

# run python
CMD ["python", "--version"]