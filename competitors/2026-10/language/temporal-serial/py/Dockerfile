FROM python:3.11-slim
RUN pip install --no-cache-dir temporalio
WORKDIR /app
COPY harness.py reader.py fibotime.py /app/
