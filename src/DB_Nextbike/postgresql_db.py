import os

from sqlalchemy import create_engine

from DB_Nextbike.models_db import Base

DB_USER = os.getenv("POSTGRES_USER", "NicoLarson")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "DataScientist123")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "nextbike")

DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

engine = create_engine(DATABASE_URL, echo=False)


"""
    Creates all database tables if they do not already exist.

    Returns:
        None
"""
def init_db():
    Base.metadata.create_all(engine)