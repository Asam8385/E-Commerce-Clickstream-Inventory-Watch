FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY clickstream_producer.py .

CMD ["python", "-u", "clickstream_producer.py"]
