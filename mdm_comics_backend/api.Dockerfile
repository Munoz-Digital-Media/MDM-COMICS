# MDM Comics API Server
FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for caching
COPY mdm_comics_backend/requirements.txt ./mdm_comics_backend/

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r mdm_comics_backend/requirements.txt

# Copy application code - preserve directory structure
COPY mdm_comics_backend/ ./mdm_comics_backend/

# Set working directory to root to align with Railway config-as-code path
WORKDIR /app

# Ensure Python can import the project package.
ENV PYTHONPATH=/app/mdm_comics_backend
# Railway sets PORT env var
ENV PORT=8000
EXPOSE 8000

CMD ["python", "mdm_comics_backend/scripts/startup.py"]
