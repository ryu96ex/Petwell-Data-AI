FROM python:3.10-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8080
ENV PORT=8080

CMD exec gunicorn \
  --bind :$PORT \
  --workers 1 \
  --threads 1 \
  --timeout 90 \
  --max-requests 200 \
  --max-requests-jitter 50 \
  --access-logfile - \
  --error-logfile - \
  -k uvicorn.workers.UvicornWorker \
  app:app
