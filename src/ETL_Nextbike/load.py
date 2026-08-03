import json
import logging

import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert

from DB_Nextbike.postgresql_db import engine
from DB_Nextbike.models_db import City, Station, TaskStationSnapshot
from Kafka_Nextbike.producer import publish_snapshots

logger = logging.getLogger(__name__)

"""
    Inserts or updates data in a database table.

    Args:
        conn: The active database connection.
        table: The database table to update.
        df: The DataFrame containing the data to insert.
        key_columns: The columns used to detect conflicts.

    Returns:
        None
"""
def _upsert_into_db(conn, table, df, key_columns):
    if df.empty:
        return

    stmt = pg_insert(table).values(df.to_dict(orient="records"))

    update_columns = {
        col.name: stmt.excluded[col.name]
        for col in table.columns
        if col.name not in key_columns
    }

    stmt = stmt.on_conflict_do_update(
        index_elements=key_columns,
        set_=update_columns,
    )

    conn.execute(stmt)

"""
    Loads processed Nextbike data into the database and publishes snapshots.

    Args:
        processed_file: The path to the processed JSON file.

    Returns:
        None

    Raises:
        Exception: If database insertion fails.
"""
def load_nextbike_processed(processed_file):

    logger.info("Loading processed file: %s", processed_file)

    with open(processed_file, "r", encoding="utf-8") as file:
        payload = json.load(file)

    cities_df = pd.DataFrame(payload.get("cities", []))
    stations_df = pd.DataFrame(payload.get("stations", []))
    snapshots_df = pd.DataFrame(payload.get("snapshots", []))

    if not snapshots_df.empty:
        snapshots_df["timestamp"] = pd.to_datetime(snapshots_df["timestamp"], utc=True)

    logger.info(
        "%d cities / %d stations / %d snapshots ready for insertion",
        len(cities_df), len(stations_df), len(snapshots_df),
    )

    try:
        with engine.begin() as conn:
            _upsert_into_db(conn, City.__table__, cities_df, ["city_id"])
            _upsert_into_db(conn, Station.__table__, stations_df, ["station_id"])

            if not snapshots_df.empty:
                stmt = pg_insert(TaskStationSnapshot.__table__).values(
                    snapshots_df.to_dict(orient="records")
                )
                stmt = stmt.on_conflict_do_nothing(
                    index_elements=["station_id", "timestamp"]
                )
                conn.execute(stmt)

    except Exception:
        logger.exception("Database insertion failed")
        raise

    active_station_ids = set(stations_df.loc[stations_df["is_active"], "station_id"])
    snapshots_to_publish = snapshots_df[snapshots_df["station_id"].isin(active_station_ids)]

    if len(snapshots_to_publish) < len(snapshots_df):
        logger.info(
            "Filtered out %d snapshot(s) from inactive stations before publishing",
            len(snapshots_df) - len(snapshots_to_publish),
        )

    try:
        publish_snapshots(snapshots_to_publish)
    except Exception:
        logger.exception("Kafka publication failed (data remains in the database)")

    logger.info("Snapshots loaded: %d (published: %d)", len(snapshots_df), len(snapshots_to_publish))
    logger.info("Load complete.")