FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose port 8080
EXPOSE 8080

# Set the PORT environment variable
ENV PORT=8080

CMD ["gunicorn", "-b", ":8080", "app:app"]
