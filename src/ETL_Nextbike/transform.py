import json
import logging
import zlib
import pandas as pd
import os

from datetime import datetime, UTC
from pathlib import Path


logger = logging.getLogger(__name__)

"""
   Generates a unique identifier for a city.

    Args:
        country: The country name of the city.
        city_name: The name of the city.

    Returns:
        int: A unique city identifier.
"""
def generate_city_id(country, city_name):
    key = f"{country}|{city_name}".encode("utf-8")
    return zlib.crc32(key) & 0x7FFFFFFF

"""
    Converts raw Nextbike data (already in memory) into structured and cleaned data.

    Args:
        raw_data: The raw data stream from the extraction.

    Returns:
        str: The path to the generated, processed JSON file.
"""
def transform_nextbike_raw(raw_data):

    logger.info("Starting transformation of in-memory raw data")

    execution_time = datetime.now(UTC)

    data = raw_data  

    cities = {}
    stations = {}
    snapshots = []

    countries = data.get("countries", [])
    logger.info("Countries found: %d", len(countries))

    for country in countries:

        for city in country.get("cities", []):
            city_name = city.get("name")

            country_name = city.get("country_name")

            if city_name is None or country_name is None:
                continue

            city_id = generate_city_id(country_name, city_name)

            cities[city_id] = {
                "city_id": city_id,
                "city_name": city_name,
                "country": country_name,
            }

            for place in city.get("places", []):
                station_id = place.get("uid")
                if station_id is None:
                    continue

                stations[station_id] = {
                    "station_id": station_id,
                    "name": place.get("name"),
                    "latitude": place.get("lat"),
                    "longitude": place.get("lng"),
                    "capacity": place.get("bike_racks"),
                    "city_id": city_id,
                    "is_active":  bool(place.get("active_place", 1)),
                    "critical_bike_threshold": 2,
                }

                bikes = place.get("bikes")
                free_racks = place.get("free_racks")

                snapshots.append(
                    {
                        "timestamp": execution_time,
                        "station_id": station_id,
                        "available_bikes": bikes,
                        "free_racks": free_racks,
                        "total_bikes": (bikes or 0) + (free_racks or 0),
                        "maintenance":  bool(place.get("maintenance", False)),
                    }
                )

    cities_df = pd.DataFrame(cities.values())
    stations_df = pd.DataFrame(stations.values())
    snapshots_df = pd.DataFrame(snapshots)

    logger.info(
        "Raw extraction: %d cities, %d stations, %d snapshots",
        len(cities_df), len(stations_df), len(snapshots_df),
    )

    for column in ["latitude", "longitude"]:
        stations_df[column] = pd.to_numeric(stations_df[column], errors="coerce")

    snapshots_df.dropna(subset=["station_id", "available_bikes", "free_racks", "total_bikes"], inplace=True)
    stations_df.drop_duplicates(subset=["station_id"], keep="last", inplace=True)

    for column in ["available_bikes", "free_racks", "total_bikes"]:
        snapshots_df[column] = pd.to_numeric(snapshots_df[column], errors="coerce")

    snapshots_df.dropna(subset=["station_id", "available_bikes", "free_racks"], inplace=True)

    snapshots_df = snapshots_df[
        (snapshots_df["available_bikes"] >= 0) & (snapshots_df["free_racks"] >= 0)
    ]

    snapshots_df = snapshots_df[
        snapshots_df["total_bikes"]
        ==
        snapshots_df["available_bikes"]
        +
        snapshots_df["free_racks"]
    ]

    valid_station_ids = set(stations_df["station_id"])
    snapshots_df = snapshots_df[snapshots_df["station_id"].isin(valid_station_ids)]

    logger.info("After cleaning: %d cities, %d stations, %d snapshots", len(cities_df), len(stations_df), len(snapshots_df),)

    BASE_DIR = Path(__file__).resolve().parent.parent.parent
    processed_folder = BASE_DIR / "data" / "processed"
    processed_folder.mkdir(parents=True, exist_ok=True)

    output_file = processed_folder / "temp_processed.json"
    tmp_output_file = processed_folder / "temp_processed.json.tmp"

    if not snapshots_df.empty:
        snapshots_df["timestamp"] = snapshots_df["timestamp"].apply(
            lambda ts: ts.isoformat() if hasattr(ts, "isoformat") else ts
        )

    payload = {
            "cities": cities_df.to_dict(orient="records"),
            "stations": stations_df.to_dict(orient="records"),
            "snapshots": snapshots_df.to_dict(orient="records"),
        }

    with open(tmp_output_file, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False)

    os.replace(tmp_output_file, output_file)

    logger.info("Processed file created: %s", output_file)

    return str(output_file)