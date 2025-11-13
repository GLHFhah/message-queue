FROM python:3.12-slim

RUN apt-get update && apt-get install -y curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR server
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python3", "-u", "server.py"]
