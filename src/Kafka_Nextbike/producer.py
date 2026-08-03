import json
import logging
import os
from datetime import date, datetime

from kafka import KafkaProducer

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_SNAPSHOTS = "nextbike.station.snapshots"

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
    Returns the Kafka producer instance.

    Returns:
        KafkaProducer: The initialized Kafka producer.
"""
def get_producer() -> KafkaProducer:
    global _producer
    if _producer is None:
        logger.info("Initializing Kafka producer")
        _producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=_json_default).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            retries=3,
            linger_ms=50,
        )

    logger.info("Kafka producer initialized successfully with server %s",KAFKA_BOOTSTRAP_SERVERS)
    
    return _producer

"""
    Publishes station snapshots to the Kafka topic.

    Args:
        snapshots_df: A DataFrame containing the station snapshots.

    Returns:
        int: The number of published snapshots.
"""
def publish_snapshots(snapshots_df) -> int:
    if snapshots_df.empty:
        logger.warning("No snapshots to publish")
        return 0

    producer = get_producer()
    count = 0

    for record in snapshots_df.to_dict(orient="records"):
        producer.send(
            TOPIC_SNAPSHOTS,
            key=record["station_id"],
            value=record,
        )
        count += 1

    producer.flush()
    logger.info( "Successfully published %d snapshots to topic %s", count, TOPIC_SNAPSHOTS)

    return count