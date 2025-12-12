FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

COPY mdm_comics_middleware/requirements.txt requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY mdm_comics_middleware/app app

ENV PORT=4100
EXPOSE 4100

CMD uvicorn app.main:app --host 0.0.0.0 --port ${PORT}
