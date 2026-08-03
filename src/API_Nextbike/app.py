import os
import logging

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.encoders import jsonable_encoder

from sqlalchemy import create_engine, text



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

logger = logging.getLogger(__name__)



app = FastAPI(
    title="NextBike Monitoring Dashboard",
    description="Dashboard displaying NextBike station alerts.",
    version="1.0.0",
)



templates = Jinja2Templates(
    directory="src/API_Nextbike/templates"
)


DB_USER = os.getenv("POSTGRES_USER", "NicoLarson")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "DataScientist123")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "nextbike")

DATABASE_URL = (
    f"postgresql+psycopg2://"
    f"{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)


logger.info(
    "Connecting to PostgreSQL database %s on %s:%s",
    DB_NAME,
    DB_HOST,
    DB_PORT,
)


engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True
)


"""
    Displays the dashboard with the latest alerts.

    Args:
        request: The HTTP request used to render the template.

    Returns:
        HTMLResponse: The rendered dashboard page.
"""
@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request):

    logger.info("Loading dashboard page")

    query = text(
        """
        SELECT
            a.timestamp,
            s.name,
            c.city_name,
            a.alert_type,
            a.message

        FROM task_alert a

        LEFT JOIN station s
            ON a.station_id = s.station_id

        LEFT JOIN city c
            ON s.city_id = c.city_id

        ORDER BY a.timestamp DESC

        LIMIT 500
        """
    )


    try:

        with engine.connect() as connection:

            alerts = connection.execute(query).fetchall()


        logger.info(
            "%s alerts loaded",
            len(alerts)
        )


        return templates.TemplateResponse(
            "index.html",
            {
                "request": request,
                "alerts": alerts,
            }
        )


    except Exception as error:

        logger.exception(
            "Error while loading dashboard: %s",
            error
        )

        return HTMLResponse(
            content="""
            <h1>Database connection error</h1>
            <p>Unable to load alerts.</p>
            """,
            status_code=500,
        )

"""
    Returns the latest alerts as JSON.

    Returns:
        list: A list of alerts in JSON format.
"""
@app.get("/api/alerts")
def get_alerts():

    query = text(
        """
        SELECT
            a.timestamp,
            s.name,
            c.city_name,
            a.alert_type,
            a.message

        FROM task_alert a

        LEFT JOIN station s
            ON a.station_id = s.station_id

        LEFT JOIN city c
            ON s.city_id = c.city_id

        ORDER BY a.timestamp DESC

        LIMIT 500
        """
    )

    try:

        with engine.connect() as connection:

            rows = connection.execute(query).mappings().all()

        logger.info(
            "%s alerts returned by API",
            len(rows)
        )

        return jsonable_encoder(
            [dict(row) for row in rows]
        )

    except Exception as error:

        logger.exception(
            "API error: %s",
            error
        )

        return JSONResponse(
            content={
                "error": "Unable to retrieve alerts"
            },
            status_code=500
        )

"""
    Checks if the service is running.

    Returns:
        dict: The health status of the service.
"""
@app.get("/health")
def health():

    return {
        "status": "ok",
        "service": "nextbike-dashboard"
    }