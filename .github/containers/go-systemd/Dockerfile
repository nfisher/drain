FROM golang:1.26.8-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends libsystemd-dev pkg-config \
    && rm -rf /var/lib/apt/lists/*
