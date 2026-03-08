FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose port 8080
EXPOSE 8080

# Set the PORT environment variable
ENV PORT=8080

# Recycle worker periodically to mitigate memory growth / fragmentation.
CMD exec gunicorn \
  --bind :$PORT \
  --workers 1 \
  --threads 1 \
  --timeout 90 \
  --max-requests 200 \
  --max-requests-jitter 50 \
  --access-logfile - \
  --error-logfile - \
  app:app
