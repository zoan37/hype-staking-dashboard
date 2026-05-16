FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py db.py ingest.py ./
COPY static ./static

ENV PORT=8080 \
    AUTO_REFRESH=1 \
    REFRESH_INTERVAL_S=3600 \
    PYTHONUNBUFFERED=1

EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
