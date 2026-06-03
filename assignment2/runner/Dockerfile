FROM eclipse-temurin:8-jdk-jammy

RUN apt-get update \
    && apt-get install -y --no-install-recommends maven bash git procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace
