FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash curl \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir happybase thriftpy2

WORKDIR /workspace
