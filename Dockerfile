# --- STAGE 1: Build the environment ---
FROM python:3.11-slim as builder

WORKDIR /usr/src/app
# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /usr/src/app/wheels -r requirements.txt

# --- STAGE 2: Create the final production image ---
FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy the built wheels from the builder stage and install them
COPY --from=builder /usr/src/app/wheels /wheels
COPY --from=builder /usr/src/app/requirements.txt .
RUN pip install --no-cache-dir --find-links=/wheels -r requirements.txt

# Install Prefect
RUN pip install "prefect[docker]>=2.15.2"

# Copy all source code directories into the container
COPY . .

# Expose ports for FastAPI and Prometheus metrics
EXPOSE 8000 9102 9103 9104 9105 9106 9107 9108 9109 9110