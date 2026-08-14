from tensorflow.keras import layers, Model

"""
    Builds a GRU model that forecasts the next `horizon` steps of the
    available-bikes ratio, conditioned on a per-station embedding.

    Args:
        num_stations: Number of distinct stations known to the model.
        seq_len: Number of input timesteps.
        n_features: Number of features per timestep.
        horizon: Number of future timesteps to predict.
        embedding_dim: Size of the station embedding vector.
        gru_units: Number of units in the GRU layer.

    Returns:
        Model: A compiled Keras model.
"""
def build_model(num_stations, seq_len, n_features, horizon, embedding_dim=8, gru_units=32):
    sequence_input = layers.Input(shape=(seq_len, n_features), name="sequence")
    station_input = layers.Input(shape=(1,), name="station_idx")

    station_embedding = layers.Embedding(input_dim=num_stations, output_dim=embedding_dim)(station_input)
    station_embedding = layers.Flatten()(station_embedding)

    x = layers.GRU(gru_units)(sequence_input)
    x = layers.Concatenate()([x, station_embedding])
    x = layers.Dense(32, activation="relu")(x)
    output = layers.Dense(horizon, activation="sigmoid", name="ratio_forecast")(x)

    model = Model(inputs=[sequence_input, station_input], outputs=output)
    model.compile(optimizer="adam", loss="mse", metrics=["mae"])
    return model