FROM python:3.11-slim

RUN apt-get update && apt-get install -y gcc g++ build-essential && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "backend_api:app", "--host", "0.0.0.0", "--port", "10000"]
