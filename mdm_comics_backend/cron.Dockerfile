FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install backend dependencies so cron jobs can reuse the same app modules
COPY mdm_comics_backend/requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy backend application and cron runner
COPY mdm_comics_backend/ .

CMD ["python", "run_cron.py"]
