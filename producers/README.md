# Activity Producer

Python Kafka producer for the `raw_activities` topic with Avro serialization.

## Features

- CLI-based activity producer with configurable options
- Avro serialization with Schema Registry integration
- Generates realistic marketing activity data (emails, web visits, forms, webinars, paid clicks)
- Environment variable configuration
- Managed with `uv`

## Prerequisites

- Python 3.10+
- `uv` package manager
- Confluent Cloud: Kafka cluster and Schema Registry credentials

## Setup

1. **Install dependencies**:
   ```bash
   cd producers
   uv sync
   ```

2. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your Confluent Cloud credentials
   ```

## Usage

### CLI Command

```bash
# Produce 10 activities (default)
uv run produce-activities

# Produce 100 activities starting from ID 4000
uv run produce-activities --count 100 --start-id 4000

# Produce to specific lead, with delay between messages
uv run produce-activities --count 50 --lead-id 1005 --interval 0.5
```

### Options

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--count` | `-c` | 10 | Number of activities to produce |
| `--start-id` | `-s` | 5000 | Starting activity_id |
| `--lead-id` | `-l` | random (1001-1010) | Fixed lead_id for all activities |
| `--interval` | `-i` | 0.0 | Seconds to wait between messages |

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | Kafka broker endpoint |
| `KAFKA_SASL_USERNAME` | Yes | Cluster API key |
| `KAFKA_SASL_PASSWORD` | Yes | Cluster API secret |
| `SCHEMA_REGISTRY_URL` | Yes | Schema Registry URL |
| `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO` | Yes | `SR_KEY:SR_SECRET` |
| `KAFKA_TOPIC` | No | Target topic (default: `raw_activities`) |

## Generated Data

The producer generates realistic activity records matching the `raw_activities` Flink table schema:

- **Activity types**: Open Email, Click Link, Visit Webpage, Fill Out Form, Register Webinar, Attended Webinar, Paid Click, Form Abandon
- **Metadata**: Includes `program_id` (matching active programs 2001-2009), channel, campaign IDs
- **Timestamps**: Random within the last hour (can be overridden programmatically)

## Domain Model

See `models.py` for the domain entities:
- `Lead`: Primary identity with m_id, email, attributes
- `ActivityRecord`: Event stream record
- `MarketingProgram`: Campaign/program context

## Integration

The producer writes to the same `raw_activities` topic consumed by Flink SQL pipelines. Produced records flow through:

```
raw_activities → src_activities → dim_activities → fct_nb_act_per_pgm
```

Verify in Confluent Cloud UI or via the consumer dashboard at `consumers/`.
