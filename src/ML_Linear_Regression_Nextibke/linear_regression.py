import logging
from datetime import datetime, timedelta, UTC

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy import text

from DB_Nextbike.postgresql_db import engine
from DB_Nextbike.models_db import TaskAlertML

logger = logging.getLogger(__name__)

WINDOW_MINUTES = 90
MIN_POINTS = 4
MIN_CONFIDENCE = 0.5
MODEL_NAME = "linear_regression"

"""
    Fetches recent, non-maintenance snapshots for all active stations,
    joined with their critical bike threshold.

    Returns:
        pd.DataFrame: One row per snapshot, with station_id, timestamp,
                      available_bikes, critical_bike_threshold.
"""
def fetch_recent_history():
    since = datetime.now(UTC) - timedelta(minutes=WINDOW_MINUTES)

    query = text(
        """
        SELECT s.station_id, s.timestamp, s.available_bikes,
               st.critical_bike_threshold
        FROM task_station_snapshot s
        JOIN station st ON st.station_id = s.station_id
        WHERE s.timestamp >= :since
          AND s.maintenance = false
          AND st.is_active = true
        ORDER BY s.station_id, s.timestamp
        """
    )

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"since": since})

    return df

"""
    Fits a linear trend on a single station's recent history and
    extrapolates the time remaining before it hits its critical threshold.

    Args:
        group: DataFrame of snapshots for a single station, sorted by time.
        threshold: The station's critical_bike_threshold.

    Returns:
        dict or None: Prediction details, or None if the trend is
                      insufficient or not statistically reliable.
"""
def predict_station(group, threshold):
    if len(group) < MIN_POINTS:
        return None

    t0 = group["timestamp"].iloc[0]
    X = (group["timestamp"] - t0).dt.total_seconds().to_numpy() / 60
    y = group["available_bikes"].to_numpy()

    slope, intercept, r_value, _, _ = stats.linregress(X, y)
    confidence = r_value ** 2

    if slope >= 0 or confidence < MIN_CONFIDENCE:
        return None

    t_threshold = (threshold - intercept) / slope
    minutes_remaining = t_threshold - X[-1]

    if minutes_remaining < 0:
        return None

    last_timestamp = group["timestamp"].iloc[-1]
    estimated_empty_at = last_timestamp + timedelta(minutes=minutes_remaining)

    return {
        "minutes_remaining": round(minutes_remaining, 1),
        "estimated_empty_at": estimated_empty_at,
        "slope": round(slope, 4),
        "confidence": round(confidence, 3),
    }

"""
    Computes depletion predictions for every station with enough recent
    history, and stores the results in task_alert_ml.

    Returns:
        int: The number of predictions inserted.
"""
def run_ml_prediction():
    logger.info("Fetching recent snapshot history (window=%d min)", WINDOW_MINUTES)
    df = fetch_recent_history()

    if df.empty:
        logger.info("No recent snapshot data available, skipping prediction")
        return 0

    computed_at = datetime.now(UTC)
    predictions = []

    for station_id, group in df.groupby("station_id"):
        threshold = group["critical_bike_threshold"].iloc[0]
        result = predict_station(group, threshold)

        if result is None:
            continue

        predictions.append(
            TaskAlertML(
                computed_at=computed_at,
                station_id=station_id,
                model=MODEL_NAME,
                estimated_empty_at=result["estimated_empty_at"],
                minutes_remaining=result["minutes_remaining"],
                slope=result["slope"],
                confidence=result["confidence"],
                message=(
                    f"Station {station_id}: estimated to run out of bikes "
                    f"in ~{result['minutes_remaining']:.0f} min "
                    f"(confidence={result['confidence']:.2f})"
                ),
            )
        )

    if not predictions:
        logger.info("No reliable depletion trend found across stations")
        return 0

    with engine.begin() as conn:
        for pred in predictions:
            conn.execute(
                TaskAlertML.__table__.insert().values(
                    computed_at=pred.computed_at,
                    station_id=pred.station_id,
                    model=pred.model,
                    estimated_empty_at=pred.estimated_empty_at,
                    minutes_remaining=pred.minutes_remaining,
                    slope=pred.slope,
                    confidence=pred.confidence,
                    message=pred.message,
                )
            )

    logger.info("Inserted %d ML predictions", len(predictions))
    return len(predictions)