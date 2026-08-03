# Nextbike-Data-Scientist-Training
The goal of this project is to learn how to carry out a data scientist project.
First, we will create a database to store the data, then use Apache Airflow to
set up the ETL process. I will process the data in real time using Kafka and
use a dashboard to present it in a more meaningful way.

## Docker 
Step by step :

    1) docker compose down -v

    2) docker compose pull

    3) docker compose up

Problem with API dashboard : 

    1) docker compose build --no-cache dashboard
    
    2) docker compose up dashboard

## Nextbike
Source: https://maps.nextbike.net/maps/nextbike-live.json
Nextbike is a bike-sharing service available in more than 20 countries. It
allows users to rent regular or electric bikes via a mobile app by scanning
a QR code, then return them to designated stations.
> No sensitive data is stored in this project (email, address, phone number, etc.)

## Database
```
In this package, I create the database architecture.
I chose to use PostgreSQL because it uses MVCC (Multi-Version Concurrency
Control), a mechanism that avoids conflicts between writing and reading data
from the same source at the same time — which is especially useful for
continuous Kafka streaming.

Tables:
City: represents a city. It has a relationship with Station through city_id.
Station: represents a station. It is linked to City, Snapshot, and Alert.
TaskStationSnapshot: represents the snapshot of each station at a given
    moment T. It is linked to Station.
TaskAlert: represents an alert. It is linked to Station.
```

## Apache Airflow DAG
```
In this package, I build the DAG (Directed Acyclic Graph) for my project.
I chose Airflow to orchestrate the tasks.

Apache Airflow is an open-source tool used to schedule and monitor workflows.
It lets you define tasks and their dependencies as a DAG, then runs them
automatically in the right order.
```

## Kafka
```
In this package, I use Kafka to stream my data.

Apache Kafka is an open-source tool used to send and receive data in
real time between different parts of a system. It works like a message
queue: one part of the system (the "producer") sends data, and another
part (the "consumer") reads it, continuously and in the right order.
```

## FastAPI
```
In this package, I use FastAPI to display the data in a more readable way
than in a terminal.

FastAPI is a modern Python web framework used to build APIs quickly. It is
based on standard Python type hints, which makes it easy to use and lets it
automatically generate interactive documentation for the API.

Routes:
    > @app.get("/", response_class=HTMLResponse): shows all the alerts.
    > @app.get("/api/alerts"): shows the alerts in JSON format.
    > @app.get("/health"): checks whether the process is running or not.
```
