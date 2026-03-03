""" 
main.py

Purpose: FastAPI application entry point. File that uvicorn runs to start
the API server

FastAPI automatically generates interactive documentation at:
    /docs   -> Swagger UI (interactive, test endpoints in the browser)
    /redoc  -> ReDoc (clean read-only documentation)
"""

import logging
import os
from pathlib import Path

import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

import sys

from api.routers import neighbourhoods
from api.models import PipelineHealth

logger = logging.getLogger(__name__)

# ----CONFIG----
# Same BASE_DATA_PATH pattern used throughout the pipeline
BASE_DATA_PATH = Path(os.environ.get("BASE_DATA_PATH", "/app/data"))
GOLD_PATH = BASE_DATA_PATH / "gold" / "neighbourhood_summary"

# ----APP SETUP----

# title, desc, and version for /docs page
app = FastAPI(
    title="Vancouver Housing + Transit API",
    description="""
        Explores the relationship between SkyTrain proximity and property values
        across Vancouver neighbourhoods.

        **Data sources:**
        - Vancouver Open Data Portal (Property Tax Reports)
        - TransLink GTFS Feed (SkyTrain Stations)

        **Pipeline:** PySpark Bronze -> Silver -> Gold -> FastAPI

        **Try the endpoints below** to explore neighbourhood stats.
    """,
    version="1.0.0",
)

# ----CORS----
# allow all routes but change in production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----ROUTERS----
# Register all the neighbourhoods router
app.include_router(neighbourhoods.router)

# ----System Endpoints----


@app.get("/health", response_model=PipelineHealth, tags=["System"])
def health_check():
    """
    Verfies the API is running and gold data is available

    Returns 'healthy' if gold parquet file exits, 'degraded' if not
    """
    overview_path = GOLD_PATH / "neighbourhood_overview"
    parquet_files = (
        list(overview_path.glob("*.parquet")) if overview_path.exists() else []
    )
    gold_available = len(parquet_files) > 0

    neighbourhood_count = None
    if gold_available:
        try:
            df = pd.read_parquet(parquet_files[0])
            neighbourhood_count = len(df)
        except Exception as e:
            logger.warning("Could not read gold data for health check: %s", e)

    return PipelineHealth(
        status="healthy" if gold_available else "degraded",
        gold_data_available=gold_available,
        neighbourhood_count=neighbourhood_count,
        message=(
            "API is running and data is available"
            if gold_available
            else "API is running but pipeline has not been run yet. Trigger it in Airflow at :8080"
        ),
    )


@app.get("/", tags=["System"])
def root():
    """Root endpoint — points you to the right places."""

    return {
        "message": "Vancouver Housing + Transit API",
        "docs": "/docs",
        "health": "/health",
        "neighbourhoods": "/api/neighbourhoods",
    }
