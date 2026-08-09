import json
import logging
import os
import time
from datetime import date, datetime

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_SNAPSHOTS = "nextbike.station.snapshots"

FLUSH_TIMEOUT_SECONDS = 30
MAX_RETRY_DELAY_SECONDS = 60
INITIAL_RETRY_DELAY_SECONDS = 2

_producer: KafkaProducer | None = None


"""
    Converts unsupported objects into JSON-serializable values.

    Args:
        value: The object to serialize.

    Returns:
        str: The serialized value.

    Raises:
        TypeError: If the object cannot be serialized.
"""
def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(f"Non-serializable type: {type(value)}")


"""
    Returns the Kafka producer instance, creating it if needed and
    retrying with exponential backoff if the broker is not yet reachable.

    Returns:
        KafkaProducer: The initialized Kafka producer.
"""
def get_producer() -> KafkaProducer:
    global _producer
    if _producer is not None:
        return _producer

    delay = INITIAL_RETRY_DELAY_SECONDS
    attempt = 1

    while True:
        try:
            logger.info(
                "Initializing Kafka producer (attempt %d): %s",
                attempt, KAFKA_BOOTSTRAP_SERVERS,
            )
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, default=_json_default).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                retries=3,
                acks="all",
                linger_ms=50,
            )
            logger.info("Kafka producer initialized successfully")
            return _producer

        except NoBrokersAvailable:
            logger.warning(
                "No Kafka broker available, retrying in %ds (attempt %d)",
                delay, attempt,
            )
        except KafkaError:
            logger.exception(
                "Unexpected Kafka error while initializing producer, retrying in %ds (attempt %d)",
                delay, attempt,
            )

        time.sleep(delay)
        delay = min(delay * 2, MAX_RETRY_DELAY_SECONDS)
        attempt += 1


"""
    Publishes station snapshots to the Kafka topic. Records that fail to
    serialize or send are skipped and logged individually, so a single
    bad record does not block the rest of the batch.

    Args:
        snapshots_df: A DataFrame containing the station snapshots.

    Returns:
        int: The number of successfully acknowledged snapshots.
"""
def publish_snapshots(snapshots_df) -> int:
    if snapshots_df.empty:
        logger.warning("No snapshots to publish")
        return 0

    producer = get_producer()
    futures = {}

    for record in snapshots_df.to_dict(orient="records"):
        station_id = record.get("station_id")
        try:
            future = producer.send(TOPIC_SNAPSHOTS, key=station_id, value=record)
            futures[station_id] = future
        except (TypeError, KafkaError):
            logger.exception(
                "Failed to queue snapshot for station %s, skipping", station_id
            )

    try:
        producer.flush(timeout=FLUSH_TIMEOUT_SECONDS)
    except KafkaError:
        logger.exception("Kafka flush failed or timed out")

    count = 0
    for station_id, future in futures.items():
        if future.succeeded():
            count += 1
        else:
            logger.error("Snapshot for station %s was not acknowledged by Kafka", station_id)

    logger.info(
        "Published %d/%d snapshots to topic %s",
        count, len(futures), TOPIC_SNAPSHOTS,
    )
    return count