"""CLI for producing activity records to Kafka."""

import logging
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import click
from dotenv import load_dotenv

from activity_producer.config import ProducerConfig
from activity_producer.producer import ActivityProducer

# Load .env file if present
env_path = Path(__file__).resolve().parent.parent.parent / ".env"
if env_path.exists():
    load_dotenv(env_path)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)


# Sample data for realistic activity generation
ACTIVITY_TYPES = [
    "Open Email",
    "Click Link",
    "Visit Webpage",
    "Fill Out Form",
    "Register Webinar",
    "Attended Webinar",
    "Paid Click",
    "Form Abandon",
]

EMAIL_CAMPAIGNS = [
    "Q1_Product_Update_Newsletter",
    "Customer Stories Digest",
    "Nurture Stream A — Product",
    "Post-webinar nurture",
]

WEB_PAGES = [
    "/products/hcm/overview",
    "/resources/whitepaper-analytics",
    "/events/roadshow-sf",
    "/demo.html",
]

PROGRAMS = [2001, 2002, 2003, 2004, 2005, 2006, 2008, 2009]  # Active/Completed programs


def generate_activity(activity_id: int, lead_id: int) -> dict:
    """Generate a random activity record."""
    activity_type = random.choice(ACTIVITY_TYPES)

    if "Email" in activity_type:
        primary_value = random.choice(EMAIL_CAMPAIGNS)
        metadata = {
            "program_id": str(random.choice(PROGRAMS)),
            "campaignId": str(random.randint(5000, 6000)),
            "channel": "Email",
        }
    elif "Webpage" in activity_type or "Form" in activity_type:
        primary_value = random.choice(WEB_PAGES)
        metadata = {
            "program_id": str(random.choice(PROGRAMS)),
            "referrer": random.choice(["google", "direct", "linkedin"]),
            "channel": "Web",
        }
    elif "Webinar" in activity_type:
        primary_value = f"HR Summit Webinar — {datetime.now().strftime('%B')}"
        metadata = {
            "program_id": str(random.choice(PROGRAMS)),
            "session_id": f"HR-{datetime.now().strftime('%b').upper()}-01",
            "channel": "Webinar",
        }
    elif "Paid Click" in activity_type:
        primary_value = random.choice(["brand-search-workday", "remarketing-hcm"])
        metadata = {
            "program_id": str(random.choice(PROGRAMS)),
            "keyword": random.choice(["workday hcm", "workday payroll"]),
            "channel": "Paid Search",
        }
    else:
        primary_value = "default-activity"
        metadata = {
            "program_id": str(random.choice(PROGRAMS)),
            "channel": "System",
        }

    # Random timestamp within last hour
    timestamp = datetime.now(timezone.utc) - timedelta(
        seconds=random.randint(0, 3600)
    )

    return {
        "activity_id": activity_id,
        "lead_id": lead_id,
        "activity_type": activity_type,
        "primary_attribute_value": primary_value,
        "timestamp": timestamp,
        "metadata": metadata,
    }


@click.command()
@click.option(
    "--count",
    "-c",
    default=10,
    type=int,
    help="Number of activities to produce (default: 10)",
)
@click.option(
    "--start-id",
    "-s",
    default=5000,
    type=int,
    help="Starting activity_id (default: 5000)",
)
@click.option(
    "--lead-id",
    "-l",
    default=None,
    type=int,
    help="Fixed lead_id (default: random from 1001-1010)",
)
@click.option(
    "--interval",
    "-i",
    default=0.0,
    type=float,
    help="Seconds to wait between messages (default: 0)",
)
def main(count: int, start_id: int, lead_id: int, interval: float):
    """
    Produce activity records to raw_activities topic.

    Requires environment variables:
      - KAFKA_BOOTSTRAP_SERVERS
      - KAFKA_SASL_USERNAME
      - KAFKA_SASL_PASSWORD
      - SCHEMA_REGISTRY_URL
      - SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO

    Example:
      produce-activities --count 100 --start-id 4000
    """
    try:
        config = ProducerConfig.from_env()
    except ValueError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Copy .env.example to .env and fill in your credentials")
        raise click.Abort()

    logger.info(f"Starting producer for topic: {config.topic}")
    logger.info(f"Bootstrap servers: {config.bootstrap_servers}")
    logger.info(f"Producing {count} activities starting from ID {start_id}")

    producer = ActivityProducer(config)

    try:
        for i in range(count):
            activity_id = start_id + i

            # Use fixed lead_id or random from sample range
            if lead_id is not None:
                target_lead_id = lead_id
            else:
                target_lead_id = random.randint(1001, 1010)

            activity = generate_activity(activity_id, target_lead_id)

            producer.produce_activity(**activity)

            if interval > 0 and i < count - 1:
                import time
                time.sleep(interval)

        logger.info(f"Produced {count} activities, flushing...")
        producer.flush()
        logger.info("Done!")

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
    except Exception as e:
        logger.error(f"Producer error: {e}", exc_info=True)
        raise
    finally:
        producer.close()


if __name__ == "__main__":
    main()
