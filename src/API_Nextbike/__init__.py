"""
In this package, I use FastAPI to display the data in a more readable way
than in a terminal.

FastAPI is a modern Python web framework used to build APIs quickly. It is
based on standard Python type hints, which makes it easy to use and lets it
automatically generate interactive documentation for the API.

Routes:
    > @app.get("/", response_class=HTMLResponse): shows all the alerts.
    > @app.get("/api/alerts"): shows the alerts in JSON format.
    > @app.get("/health"): checks whether the process is running or not.
"""

version = "1.0.0"