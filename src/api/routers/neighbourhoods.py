""" 
routers/neighbourhoods.py

Purpose: API endpoints related to the Vancouver neighbourhoods

Router is registered in main.py via app.include_router(neighbourhoods.router)
All endpoints present in the router are automatically prefixed with /api/neighbourhoods

Endpoints: 
    GET /api/neighbourhoods                         -> all neighbourhoods
    GET /api/neighbourhoods/{code}                  -> one neighbourhood by numeric code
    GET /api/neighbourhoods/{code}/transit-analysis -> transit proximity breakdown
"""

import logging
import os
from pathlib import Path

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

import sys
from api.models import NeighbourhoodOverview, TransitProximityDetail

logger = logging.getLogger(__name__)

# ----CONFIG----
# Reads from /app/data — the same volume the pipeline writes to
BASE_DATA_PATH = Path(os.environ.get("BASE_DATA_PATH", "/app/data"))
GOLD_PATH = BASE_DATA_PATH / "gold" / "neighbourhood_summary"

# prefix - /api/neighbourhoods means all routes start with that path
# tags - groups the apis together in the /docs Swagger UI
router = APIRouter(prefix="/api/neighbourhoods", tags=["Neighbourhoods"])

# ----DATA LOADING----


def load_overview() -> pd.DataFrame:
    """
    Load the neighbourhood overview gold table into pandas (since it only has ~30 rows)
    """

    overview_path = GOLD_PATH / "neighbourhood_overview"

    if not overview_path.exists():
        raise FileNotFoundError(
            f"Gold data not found at {overview_path}. "
            "Run the pipeline first via Airflow at :8080"
        )

    parquet_files = list(overview_path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError("No parquet files found in gold overview directory")

    dfs: list = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)

    # neighbourhood_code from Spark is an integer - cast to string
    df["neighbourhood_code"] = df["neighbourhood_code"].astype(str)

    return df


def load_transit_detail() -> pd.DataFrame:
    """
    Loads the neighbourhood + transit proximity detail gold table.
    One row per neighbourhood x proximity category combination.
    """
    detail_path = GOLD_PATH / "neighbourhood_transit_detail"

    if not detail_path.exists():
        raise FileNotFoundError(f"Gold detail data not found at {detail_path}")

    parquet_files = list(detail_path.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError("No parquet files found in gold detail directory")

    dfs: list = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True)
    df["neighbourhood_code"] = df["neighbourhood_code"].astype(str)
    return df


# ----ENDPOINTS----
@router.get("/", response_model=list[NeighbourhoodOverview])
def get_all_neighbourhoods(
    min_properties: int = Query(
        default=0, description="Filter: minimum number of properties"
    ),
    sort_by: str = Query(default="avg_land_value", description="Sort field"),
) -> list[dict]:
    """
    Returns overview stats for all Vancouver neighbourhoods

    Qeury params are parsed by FastAPI automatically from func signature

    Try:
        GET /api/neighbourhoods
        GET /api/neighbourhoods?sort_by=avg_distance_to_station_km
        GET /api/neighbourhoods?min_properties=10000&sort_by=avg_land_value
    """

    try:
        df = load_overview()
    except FileNotFoundError as e:
        logger.error("Gold data unavailable: %s", e)
        # 503 = Service Unavailable — data not ready, not a client error
        raise HTTPException(status_code=503, detail=str(e))

    # Filter
    if min_properties > 0:
        df = df[df["total_properties"] >= min_properties]

    # Sort - whitelist valid fields to prevent injection in code
    valid_sort_fields = [
        "avg_land_value",
        "total_properties",
        "avg_distance_to_station_km",
        "avg_yoy_change_pct",
        "pct_within_500m_station",
    ]

    if sort_by in valid_sort_fields:
        df = df.sort_values(sort_by, ascending=False, na_position="last")

    logger.info("Returning %d neighbourhoods (sorted by %s)", len(df), sort_by)
    return df.to_dict(orient="records")


@router.get("/{neighbourhood_code}", response_model=NeighbourhoodOverview)
def get_neighhourbood(neighbourhood_code: str) -> dict:
    """
    Returns stats for a specific neighbourhood by its numeric code

    params are extracted from the url by FastAPI like /26

    Try:
        GET /api/neighbourhoods/26  -> Downtown
        GET /api/neighbourhoods/2   -> Kitsilano
        GET /api/neighbourhoods/11  -> Cambie
    """
    try:
        df = load_overview()
    except FileNotFoundError as e:
        raise HTTPException(status_code=503, detail=str(e))

    result = df[df["neighbourhood_code"] == neighbourhood_code]

    if result.empty:
        logger.warning("Neighbourhood not found: %s", neighbourhood_code)
        raise HTTPException(
            status_code=404,
            detail=f"Neighbourhood '{neighbourhood_code}' not found. "
            "Use GET /api/neighbourhoods to see all available codes.",
        )
    return result.iloc[0].to_dict()


@router.get(
    "/{neighbourhood_code}/transit-analysis",
    response_model=list[TransitProximityDetail],
)
def get_transit_analysis(neighbourhood_code: str) -> list[dict]:
    """
    returns property value stats broken down by transit proximity for a neighbourhood
    Q: Do properties closer to the SkyTrain have higher land values?

    each row response is one proximity bucket (ex. 1km, <500m)
    with aggregated property stats for all properties in that bucket

    Try:
        GET /api/neighbourhoods/2/transit-analysis   -> Kitsilano breakdown
        GET /api/neighbourhoods/26/transit-analysis  -> Downtown breakdown
    """

    try:
        df = load_transit_detail()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    result = df[df["neighbourhood_code"] == neighbourhood_code]

    if result.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No transit data found for neighbourhood '{neighbourhood_code}'. "
            "Use GET /api/neighbourhoods to see all available codes.",
        )

    logger.info("Returning transit analysis for neighbourhood %s", neighbourhood_code)
    return result.to_dict(orient="records")
