FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python-ng
COPY producer.py .
CMD ["python", "-u", "producer.py"]
