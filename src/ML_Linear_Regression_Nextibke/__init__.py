"""
In this package, I use a rolling linear regression to predict when a
station will run out of bikes.

For each active station, I fit a linear trend on its recent snapshot
history (available_bikes over time) and extrapolate the time remaining
before it reaches the station's critical_bike_threshold.

Predictions are recomputed on every DAG run (batch, every 15 min) and
stored in task_alert_ml — there is no persisted trained model, since the
regression is refit from scratch on each execution.
"""

version = "1.0.0"