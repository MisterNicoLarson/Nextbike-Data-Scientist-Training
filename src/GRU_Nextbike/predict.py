import json
import logging
from datetime import datetime, timedelta, UTC
from pathlib import Path

import numpy as np
import tensorflow as tf

from GRU_Nextbike.data import fetch_recent_window, build_features, FEATURE_COLUMNS, SEQ_LEN, HORIZON

logger = logging.getLogger(__name__)

MODEL_DIR = Path("/app/models")

_model = None
_station_index = None

"""
    Loads the trained model and station index mapping from disk into
    memory, if they exist.

    Returns:
        bool: True if a model was successfully loaded, False otherwise.
"""
def load_model():
    global _model, _station_index

    model_path = MODEL_DIR / "gru_model.keras"
    index_path = MODEL_DIR / "station_index.json"

    if not model_path.exists() or not index_path.exists():
        _model = None
        _station_index = None
        return False

    _model = tf.keras.models.load_model(model_path)
    with open(index_path) as f:
        _station_index = json.load(f)

    return True

"""
    Returns:
        bool: Whether a trained model is currently loaded in memory.
"""
def is_model_ready():
    return _model is not None

"""
    Runs inference for a single station: fetches its recent snapshot
    window, forecasts the next HORIZON steps, and estimates when the
    station will hit its critical threshold, if within the horizon.

    Args:
        station_id: The station to predict for.

    Returns:
        dict: Prediction result, or an "error" key if unavailable.
"""
def run_inference(station_id):
    if _model is None:
        return {"error": "model_not_trained"}

    station_key = str(station_id)
    if station_key not in _station_index:
        return {"error": "unknown_station"}

    df = fetch_recent_window(station_id, window_size=SEQ_LEN)
    if len(df) < SEQ_LEN:
        return {"error": "insufficient_history"}

    capacity = df["capacity"].iloc[-1]
    threshold = df["critical_bike_threshold"].iloc[-1]

    features_df = build_features(df)
    X = features_df[FEATURE_COLUMNS].to_numpy().reshape(1, SEQ_LEN, -1)
    station_idx = np.array([[_station_index[station_key]]])

    forecast_ratio = _model.predict([X, station_idx], verbose=0)[0]
    forecast_bikes = forecast_ratio * capacity

    minutes_remaining = None
    estimated_empty_at = None

    for step, bikes in enumerate(forecast_bikes, start=1):
        if bikes <= threshold:
            minutes_remaining = step * 5
            estimated_empty_at = (
                datetime.now(UTC) + timedelta(minutes=minutes_remaining)
            ).isoformat()
            break

    message = (
        f"Station {station_id}: GRU forecast estimates depletion in ~{minutes_remaining} min"
        if minutes_remaining is not None
        else f"Station {station_id}: no depletion predicted within {HORIZON * 5} min"
    )

    return {
        "station_id": station_id,
        "minutes_remaining": minutes_remaining,
        "estimated_empty_at": estimated_empty_at,
        "forecast_bikes": [round(float(b), 1) for b in forecast_bikes],
        "message": message,
    }