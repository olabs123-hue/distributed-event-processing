FROM ubuntu:24.04 AS build

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential cmake pkg-config librdkafka-dev librdkafka++1 \
    libpqxx-dev libpq-dev ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j2

FROM ubuntu:24.04 AS runtime
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    librdkafka1 libpq5 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/build/event_producer /app/event_producer
COPY --from=build /app/build/event_worker /app/event_worker
