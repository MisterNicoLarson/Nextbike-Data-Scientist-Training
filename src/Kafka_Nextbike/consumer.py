import json
import logging
import os
import time
from collections import defaultdict, deque
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_SNAPSHOTS = "nextbike.station.snapshots"

DB_USER = os.getenv("POSTGRES_USER", "NicoLarson")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "DataScientist123")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "nextbike")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
engine = create_engine(DATABASE_URL)

WINDOW_SIZE = 2 
station_history: dict[int, deque] = defaultdict(lambda: deque(maxlen=WINDOW_SIZE))
last_alert_status: dict[int, str] = {}
last_maintenance_status: dict[int, bool] = {}
station_info_cache: dict[int, dict] = {}

MAX_RETRY_DELAY_SECONDS = 60
INITIAL_RETRY_DELAY_SECONDS = 2

"""
    Connects to the Kafka broker, retrying with exponential backoff
    until a connection is successfully established.

    Returns:
        KafkaConsumer: The connected Kafka consumer instance.
"""
def connect_to_kafka() -> KafkaConsumer:
    delay = INITIAL_RETRY_DELAY_SECONDS
    attempt = 1

    while True:
        try:
            logger.info(
                "Connecting to Kafka broker (attempt %d): %s",
                attempt, KAFKA_BOOTSTRAP_SERVERS,
            )
            consumer = KafkaConsumer(
                TOPIC_SNAPSHOTS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                auto_offset_reset="latest",
                enable_auto_commit=True,
                group_id="nextbike-alert-consumer",
            )
            logger.info("Connected to Kafka broker successfully")
            return consumer

        except NoBrokersAvailable:
            logger.warning(
                "No Kafka broker available, retrying in %ds (attempt %d)",
                delay, attempt,
            )
        except KafkaError:
            logger.exception(
                "Unexpected Kafka error while connecting, retrying in %ds (attempt %d)",
                delay, attempt,
            )

        time.sleep(delay)
        delay = min(delay * 2, MAX_RETRY_DELAY_SECONDS)
        attempt += 1

"""
    Returns station information from the database or cache.

    Args:
        station_id: The unique identifier of the station.

    Returns:
        dict: The station information.
"""
def get_station_info(station_id):
    if station_id in station_info_cache:
        return station_info_cache[station_id]

    with engine.connect() as conn:
        result = conn.execute(
            text(
                """
                SELECT station.name, city.city_name, city.country
                FROM station
                LEFT JOIN city ON station.city_id = city.city_id
                WHERE station.station_id = :station_id
                """
            ),
            {"station_id": station_id},
        ).fetchone()

    if result is None:
        info = {"name": f"#{station_id}", "city_name": "?", "country": "?"}
    else:
        info = {
            "name": result[0] or f"#{station_id}",
            "city_name": result[1] or "?",
            "country": result[2] or "?",
        }

    station_info_cache[station_id] = info
    return info

"""
    Determines the alert status based on a bike-count threshold relative
    to the station's total capacity, and estimates the time until the
    station runs out of bikes.

    Args:
        history: A list of recent station snapshots.

    Returns:
        tuple: The alert status and the estimated time until the station is empty.
"""
def compute_flexible_status(history):
    current = history[-1]
    previous = history[0] if len(history) > 1 else current

    max_bike = current.get("total_bikes") or 0
    available_bike = current["available_bikes"]

    if max_bike <= 0:
        logger.warning(
            "Station %s: invalid total_bikes value (%s), skipping status computation",
            current["station_id"], current.get("total_bikes"),
        )
        return "UNKNOWN", None

    warning_alert = max_bike / 3
    critical_alert = max_bike / 4

    if available_bike <= critical_alert:
        status = "CRITICAL"
    elif available_bike <= warning_alert:
        status = "WARNING"
    else:
        status = "OK"

    bike_delta = current["available_bikes"] - previous["available_bikes"]
    depletion_rate = max(0, -bike_delta / 5)

    estimated_empty_minutes = None
    if depletion_rate > 0:
        estimated_empty_minutes = current["available_bikes"] / depletion_rate

    return status, estimated_empty_minutes

"""
    Stores an alert in the database.

    Args:
        station_id: The unique identifier of the station.
        timestamp: The date and time of the alert.
        status: The alert status.
        message: The alert message.

    Returns:
        None
"""
def store_alert(station_id, timestamp, status, message) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO task_alert (timestamp, station_id, alert_type, message)
                VALUES (:timestamp, :station_id, :alert_type, :message)
                """
            ),
            {
                "timestamp": timestamp,
                "station_id": station_id,
                "alert_type": status,
                "message": message,
            },
        )

"""
    Processes a station snapshot and creates alerts if needed.

    Args:
        record: The station snapshot received from Kafka.

    Returns:
        None
"""
def handle_message(record):
    station_id = record["station_id"]
    record["timestamp"] = datetime.fromisoformat(record["timestamp"])

    maintenance = bool(record.get("maintenance", False))
    previous_maintenance = last_maintenance_status.get(station_id, False)

    station_info = get_station_info(station_id)
    station_label = f"{station_info['name']} ({station_info['city_name']}, {station_info['country']})"

    if maintenance and not previous_maintenance:
        message = (
            f"Station {station_label}: entered maintenance mode, "
            f"available bikes/racks count may not reflect reality"
        )
        logger.warning("Alert triggered | status=MAINTENANCE | %s", message)
        store_alert(station_id, record["timestamp"], "MAINTENANCE", message)

    elif not maintenance and previous_maintenance:
        logger.info("Station %s: maintenance ended", station_label)

    last_maintenance_status[station_id] = maintenance

    history = station_history[station_id]
    history.append(record)

    status, eta = compute_flexible_status(history)
    previous_status = last_alert_status.get(station_id, "OK")

    if status not in ("OK", "UNKNOWN") and status != previous_status:
        message = f"Station {station_label}: {int(record['available_bikes'])} bike(s) left"
        if eta is not None:
            message += f", estimated to run out in ~{eta:.0f} min"
        if maintenance:
            message += " (station under maintenance, data may be unreliable)"

        logger.warning("Alert triggered | status=%s | %s", status, message)
        store_alert(station_id, record["timestamp"], status, message)

    elif status == "OK" and previous_status != "OK":
        logger.info("Station %s returned to normal state", station_label)

    last_alert_status[station_id] = status

"""
    Starts the Kafka consumer and processes incoming messages.

    Returns:
        None
"""
def main():
    consumer = connect_to_kafka()

    logger.info("Started consuming messages from topic %s", TOPIC_SNAPSHOTS)

    for message in consumer:
        try:
            handle_message(message.value)
        except Exception:
            logger.exception("Error while processing message: %s", message.value)


if __name__ == "__main__":
    main()