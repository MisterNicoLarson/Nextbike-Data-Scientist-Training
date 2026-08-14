from sqlalchemy import (Column, Integer, Float, String, Text, TIMESTAMP, ForeignKey, UniqueConstraint,)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import BOOLEAN

Base = declarative_base()


class City(Base):
    __tablename__ = "city"

    city_id = Column(Integer, primary_key=True)
    city_name = Column(String(100), nullable=False)
    country = Column(String(50), nullable=False)

    stations = relationship( "Station",  back_populates="city")


class Station(Base):
    __tablename__ = "station"

    station_id = Column(Integer, primary_key=True)
    name = Column(String(150))
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    capacity = Column(Integer)
    city_id = Column(Integer, ForeignKey("city.city_id"))
    is_active = Column(BOOLEAN, default=True)
    critical_bike_threshold = Column(Integer, default=2)

    city = relationship("City", back_populates="stations")
    snapshots = relationship("TaskStationSnapshot", back_populates="station")
    alerts = relationship("TaskAlert", back_populates="station")
    alerts_ml = relationship("TaskAlertML", back_populates="station")

class TaskStationSnapshot(Base):
    __tablename__ = "task_station_snapshot"
    __table_args__ = (
        UniqueConstraint("station_id", "timestamp", name="uq_snapshot_station_time"),
    )

    id_TaskStationSnapshot = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)
    station_id = Column(Integer, ForeignKey("station.station_id"), nullable=False)
    available_bikes = Column(Integer, nullable=False)
    free_racks = Column(Integer, nullable=False)
    total_bikes = Column(Integer,nullable=False)
    maintenance = Column(BOOLEAN, default=False)

    station = relationship("Station", back_populates="snapshots")


class TaskAlert(Base):
    __tablename__ = "task_alert"

    id_TaskAlert = Column(Integer, primary_key=True, autoincrement=True)
    timestamp = Column(TIMESTAMP(timezone=True), nullable=False)
    station_id = Column(Integer, ForeignKey("station.station_id"), nullable=False)
    alert_type = Column(String(50), nullable=False)
    message = Column(Text)
    severity = Column(String(20))

    station = relationship("Station", back_populates="alerts")


class TaskAlertML(Base):
    __tablename__ = "task_alert_ml"

    id_TaskAlertML = Column(Integer, primary_key=True, autoincrement=True)
    computed_at = Column(TIMESTAMP(timezone=True), nullable=False)
    station_id = Column(Integer, ForeignKey("station.station_id"), nullable=False)
    model = Column(String(50), nullable=False)
    estimated_empty_at = Column(TIMESTAMP(timezone=True))
    minutes_remaining = Column(Float)
    slope = Column(Float)
    confidence = Column(Float)
    message = Column(Text)

    station = relationship("Station", back_populates="alerts_ml")