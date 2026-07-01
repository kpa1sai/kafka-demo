FROM python:3.11-slim
WORKDIR /app
RUN pip install --no-cache-dir kafka-python-ng rich
COPY monitor.py .
CMD ["python", "-u", "monitor.py"]
