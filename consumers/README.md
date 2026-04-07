# Kafka Consumers for Fact and Dimension Tables

## Features

* [x] Connect to Kafka cluster on Confluent Cloud
* [x] Present metrics in HTML gauges
* [x] Simple FastAPI backend, serving HTML business intelligence dashboard in HTML

## Overview

Python app under this directory consumes the Flink sink topic **`fct_nb_act_per_pgm`** (Avro key and value via Schema Registry, same context as the Flink table DDL). It keeps an in-memory upsert snapshot and exposes JSON metrics plus an HTML gauge dashboard.

## Prerequisites

* `uv`
* Confluent Cloud: Kafka API key, Schema Registry URL and API key (see `.env.example`)
* Topic and schemas created when the Flink pipeline for `fct_nb_act_per_pgm` is deployed

## How to run

```bash
cd consumers
cp .env.example .env
# Edit .env with your cluster bootstrap server, SR URL, and credentials

uv sync
uv run uvicorn consumer_dashboard.main:app --reload --host 0.0.0.0 --port 8080
```

Open [http://127.0.0.1:8080/](http://127.0.0.1:8080/) for the dashboard. JSON metrics: [http://127.0.0.1:8080/api/metrics](http://127.0.0.1:8080/api/metrics). Health: [http://127.0.0.1:8080/healthz](http://127.0.0.1:8080/healthz).

Alternative entry point:

```bash
uv run consumer-dashboard
```

(Starts uvicorn on `0.0.0.0:8080` without `--reload`.)

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | Broker host:port |
| `KAFKA_SASL_USERNAME` | Yes | Cluster API key |
| `KAFKA_SASL_PASSWORD` | Yes | Cluster API secret |
| `SCHEMA_REGISTRY_URL` | Yes | `https://psrc-....` |
| `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO` | Yes | `SR_KEY:SR_SECRET` |
| `KAFKA_TOPIC` | No | Default `fct_nb_act_per_pgm` |
| `KAFKA_GROUP_ID` | No | Default `wd-fct-nb-act-dashboard` |
| `KAFKA_AUTO_OFFSET_RESET` | No | Default `earliest` |

If any of the required Kafka/SR variables are missing, the API and dashboard still start but the consumer thread does not run (status pills show **Config incomplete**).
