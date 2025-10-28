FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git && rm -rf /var/lib/apt/lists/*

COPY requirements-ml.txt /requirements-ml.txt
RUN pip install --no-cache-dir -r /requirements-ml.txt

COPY ./src /app/src
ENV PYTHONPATH=/app

CMD ["python", "src/nlp/summarizer.py"]