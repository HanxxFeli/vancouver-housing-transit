"""
models.py 

Define the shape of data the API will return 

Pydantic models that will be used to define the expected structure of data 
Fast API will use them to: 
1. Validate data matches the expected format 
2. Auto-generate the /docs Swagger documentation
3. Serialize python objects to JSON
"""

from pydantic import BaseModel

class NeighbourhoodOverview(BaseModel):
    """
    Summary stats for a single Vancouver neighbourhood
    Returned by:
    - GET /api/neighbourhoods
    - GET /api/neighbourhoods/{code}
    """

    neighbourhood_code: str
    neighbourhood_name: str | None     # readable version "Kitsilano"
    total_properties: int
    avg_land_value: float | None 
    avg_distance_to_station_km: float | None
    pct_within_500m_station: float | None
    avg_yoy_change_pct: float | None

    class Config: 
        # allow the model to be created from pandas DF rows 
        from_attributes = True

class TransitProximityDetail(BaseModel):
    """
    Property value breakdown by transit proximity for a neighbourhood
    Returned by: 
    GET /api/neighbourhoods/{code}/transit-analysis
    """

    neighbourhood_code: str
    neighbourhood_name: str | None
    transit_proximity_category: str             # e.g. "< 500m", "500m - 1km"
    property_count: int
    avg_land_value: float | None
    median_land_value: float | None
    avg_distance_to_station_km: float | None
    primary_station: str | None             # nearest SkyTrain station name
    avg_yoy_change_pct: float | None

class PipelineHealth(BaseModel):
    """
    Healh check response - verifies the API is running and gold data is available
    Returned by: 
    - GET /health
    """

    status: str
    gold_data_available: bool
    neighbourhood_count: int | None
    message: str
