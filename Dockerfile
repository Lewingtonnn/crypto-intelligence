# Use a base image with Python and Prefect pre-installed
FROM prefecthq/prefect:2-python3.11

# Set the working directory
WORKDIR /app

# Install system dependencies, including 'wait-for-it.sh'
RUN apt-get update && apt-get install -y --no-install-recommends curl git && \
    curl -sL https://github.com/vishnubob/wait-for-it/raw/master/wait-for-it.sh -o /usr/local/bin/wait-for-it.sh && \
    chmod +x /usr/local/bin/wait-for-it.sh && \
    rm -rf /var/lib/apt/lists/*

# Copy your Python dependencies
COPY requirements.txt ./

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code directories into the container
COPY flows/ ./flows/
COPY ingest/ ./ingest/
COPY onchain/ ./onchain/
COPY sentiment/ ./sentiment/
COPY fastAPI/ ./fastAPI/