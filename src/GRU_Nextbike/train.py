import json
import logging
from pathlib import Path

from sklearn.model_selection import train_test_split

from GRU_Nextbike.data import fetch_training_data, build_sequences, SEQ_LEN, HORIZON, FEATURE_COLUMNS
from GRU_Nextbike.model import build_model

logger = logging.getLogger(__name__)

MODEL_DIR = Path("/app/models")
MIN_ROWS_PER_STATION = SEQ_LEN + HORIZON + 20
MIN_ELIGIBLE_STATIONS = 5

"""
    Trains the global GRU model on all eligible stations' history and
    saves the model and station index mapping to disk.

    Returns:
        dict: Training status and metrics.
"""
def run_training():
    logger.info("Fetching historical snapshot data for GRU training")
    df = fetch_training_data()

    if df.empty:
        logger.warning("No training data available")
        return {"status": "skipped", "reason": "no_data"}

    station_counts = df.groupby("station_id").size()
    eligible_stations = station_counts[station_counts >= MIN_ROWS_PER_STATION].index.tolist()

    if len(eligible_stations) < MIN_ELIGIBLE_STATIONS:
        logger.warning(
            "Not enough historical data yet (%d eligible stations, need >=%d)",
            len(eligible_stations), MIN_ELIGIBLE_STATIONS,
        )
        return {
            "status": "skipped",
            "reason": "insufficient_history",
            "eligible_stations": len(eligible_stations),
        }

    df = df[df["station_id"].isin(eligible_stations)]
    station_id_to_index = {int(sid): idx for idx, sid in enumerate(eligible_stations)}

    X, y, station_idx = build_sequences(df, station_id_to_index)
    logger.info("Built %d training sequences across %d stations", len(X), len(eligible_stations))

    X_train, X_val, y_train, y_val, s_train, s_val = train_test_split(
        X, y, station_idx, test_size=0.2, random_state=42
    )

    model = build_model(
        num_stations=len(eligible_stations),
        seq_len=SEQ_LEN,
        n_features=len(FEATURE_COLUMNS),
        horizon=HORIZON,
    )

    history = model.fit(
        [X_train, s_train], y_train,
        validation_data=([X_val, s_val], y_val),
        epochs=20,
        batch_size=64,
        verbose=2,
    )

    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model.save(MODEL_DIR / "gru_model.keras")

    with open(MODEL_DIR / "station_index.json", "w") as f:
        json.dump(station_id_to_index, f)

    val_mae = float(history.history["val_mae"][-1])
    logger.info("Training complete, val_mae=%.4f", val_mae)

    return {
        "status": "trained",
        "val_mae": val_mae,
        "stations": len(eligible_stations),
        "sequences": len(X),
    }