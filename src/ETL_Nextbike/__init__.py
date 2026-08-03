"""
In this package, I set up the ETL pipeline.

ETL stands for "Extract, Transform, and Load."

In the extraction file, I retrieve data from the Nextbike API, then pass it to the
transformation script, which extracts relevant information such as the city name,
station name, and maximum capacity of each station.

No sensitive information is collected.

In the loading script, I save the data to a database, then launch Kafka to stream
the data continuously.
"""

version = "1.0.0"