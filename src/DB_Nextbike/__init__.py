"""
In this package, I create the database architecture.
I chose to use PostgreSQL because it uses MVCC (Multi-Version Concurrency
Control), a mechanism that avoids conflicts between writing and reading data
from the same source at the same time — which is especially useful for
continuous Kafka streaming.

Tables:
    > City: represents a city. It has a relationship with Station through city_id.
    > Station: represents a station. It is linked to City, Snapshot, and Alert.
    > TaskStationSnapshot: represents the snapshot of each station at a given moment T. It is linked to Station.
    > TaskAlert: represents an alert. It is linked to Station.

Commands Postgre :
    > Docker : docker compose exec postgres psql -U NicoLarson -d nextbike
    > List tables : \dt
    > Exit : \q

    > Read Tables :
        >> SELECT * FROM city;
        >> SELECT * FROM station;
        >> SELECT * FROM task_station_snapshot;
        >> SELECT * FROM task_alert;

    > Last row insert :
        SELECT * FROM task_station_snapshot ORDER BY timestamp DESC LIMIT 10;

    > Last alert :
        SELECT * FROM task_alert ORDER BY timestamp DESC LIMIT 10;

    > NEVER FORGET ;
"""     

version = "1.0.0"