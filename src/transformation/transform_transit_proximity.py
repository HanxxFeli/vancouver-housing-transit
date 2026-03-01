"""
transform_transit_proximity.py

Purpose: For every Vancouver property, find the nearest SkyTrain station
and calculate the distance to it.

Running: 
docker compose exec pipeline python -m transformation.transform_transit_proximity
"""

import math
from pathlib import Path
import logging

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructType, StructField
from transformation.spark_session import create_spark_session

logger = logging.getLogger(__name__)

#----CONFIG----
SILVER_PROPERTIES_PATH = Path(__file__).parent.parent.parent / "data" / "silver" / "properties_cleaned"
BRONZE_STOPS_PATH = Path(__file__).parent.parent.parent / "data" / "bronze" / "translink_stops" / "skytrain_stations_raw.parquet"
OUTPUT_PATH = Path(__file__).parent.parent.parent / "data" / "silver" / "properties_with_transit"

#----Haversine Distance----

def haversine_distance(lat1: float, long1: float, lat2: float, long2: float) -> float:
    """
    calculate the great-circle (shortest path between two points in a sphere)
    distance between two coordinates in KM

    Formula: 
    1. convert deg to rad
    2. calculate differences in lat/long
    3. apply Haversine formula
    4. convert into KM using Earth radius (6371 km)

    Args:
        lat1 (float)
        long1 (float)
        lat2 (float)
        long2 (float)

    Returns:
        float: distance
    """

    # handle none values
    if any(v is None for v in [lat1, long1, lat2, long2]):
        return None
    
    # Earth's radius in KM
    R = 6371.0

    # decimal deg to rad - (x * pi / 180)
    lat1_r, long1_r = math.radians(lat1), math.radians(long1)
    lat2_r, long2_r = math.radians(lat2), math.radians(long2)

    # differences
    dif_lat = lat2_r - lat1_r
    dif_long = long2_r - long1_r

    # Haversine Formula
    a = math.sin(dif_lat / 2)**2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dif_long / 2)**2
    c = 2 * math.asin(math.sqrt(a))

    distance = round(R * c, 4)
    return distance

# UDF - slower but still appropriate. Eventually use native spark functions
haversine_udf = F.udf(haversine_distance, DoubleType())

#----Transformation----

def load_skytrain_stations(spark: SparkSession) -> DataFrame: 
    """
    Loads sky train stations for joining
    Values needed: Station name, latitude, longitude
    """

    stations = spark.read.parquet(str(BRONZE_STOPS_PATH))

    # select only needed values and rename 
    stations = stations.select( 
        F.col("stop_id").alias("station_id"),
        F.col("stop_name").alias("station_name"),
        F.col("stop_lat").cast(DoubleType()).alias("station_lat"),
        F.col("stop_lon").cast(DoubleType()).alias("station_long")
    ).dropDuplicates(["station_name"])

    logger.info("Loaded %d SkyTrain stations", stations.count())

    return stations

def calculate_nearest_station(properties: DataFrame, stations: DataFrame) -> DataFrame:
    """
    For each property, find the nearest SkyTrain station and the distance to it

    Cross Join Approach: Produces every combination of rows from 2 tables. 
    If 100,000 properties and 50 stations, cross join = 5,000,000

    for each property, calculate distance to all stations, then only keep the closest one
    (brute force spacial join)
    """

    logger.info("Performing spatial join (cross join + distance calculation)...")
    logger.info("This may take a few minutes for large datasets...")

    # Check if lat/long exists in dataset
    # Vancouver property tax data may have coors or may need to geocode (handle both)
    if "latitude" not in properties.columns or "longitude" not in properties.columns:
        logger.warning("Property data doesn't have lat/lon columns.")
        logger.info("You'll need to geocode addresses or find a version of the dataset with coordinates.")
        logger.info("For now, creating a simplified version with neighbourhood centroids...")

        # approximate center points for vancouver neighbourhoods (centroid)
        neighbourhood_coords = {
            "DOWNTOWN": (49.2827, -123.1207),
            "WEST END": (49.2888, -123.1394),
            "KITSILANO": (49.2672, -123.1638),
            "MOUNT PLEASANT": (49.2631, -123.1009),
            "FAIRVIEW": (49.2651, -123.1280),
            "CAMBIE": (49.2453, -123.1167),
            "HASTINGS-SUNRISE": (49.2791, -123.0490),
            "RENFREW-COLLINGWOOD": (49.2479, -123.0454),
            "KENSINGTON-CEDAR COTTAGE": (49.2454, -123.0793),
            "RILEY PARK": (49.2500, -123.1010),
            "GRANDVIEW-WOODLAND": (49.2739, -123.0705),
            "DUNBAR-SOUTHLANDS": (49.2361, -123.1891),
            "KERRISDALE": (49.2322, -123.1571),
            "MARPOLE": (49.2093, -123.1237),
            "SUNSET": (49.2215, -123.0856),
            "VICTORIA-FRASERVIEW": (49.2174, -123.0611),
            "KILLARNEY": (49.2200, -123.0299),
            "SOUTH CAMBIE": (49.2408, -123.1175),
            "OAKRIDGE": (49.2257, -123.1238),
            "SHAUGHNESSY": (49.2479, -123.1419),
            "ARBUTUS RIDGE": (49.2467, -123.1622),
            "WEST POINT GREY": (49.2668, -123.2010),
        }

        # Add approximate coordinates based on neighbourhood
        # Create pyspark map using F.create_map from key-value pairs
        coords_map = {}
        for neighbourhood, (lat, lon) in neighbourhood_coords.items():
            coords_map[neighbourhood] = (lat, lon)

        # User defined function handling 
        def get_neighbourhood_lat(neighbourhood_code):
            return coords_map.get(str(neighbourhood_code), (49.2827,))[0] if neighbourhood_code else None
        
        def get_neighbourhood_lon(neighbourhood_code):
            return coords_map.get(str(neighbourhood_code), (None, -123.1207,))[1] if neighbourhood_code else None
        
        get_lat_udf = F.udf(get_neighbourhood_lat, DoubleType())
        get_lon_udf = F.udf(get_neighbourhood_lon, DoubleType())

        properties = properties.withColumn("latitude", get_lat_udf(F.col("neighbourhood_code")))
        properties = properties.withColumn("longitude", get_lon_udf(F.col("neighbourhood_code")))


    #---Cross Join---
    cross = properties.crossJoin(stations)

    # logger.info("Cross join produced %s combinations", f"{cross.count():,}")

    #---Calculate Distance for each Combo---"
    cross = cross.withColumn(
        "distance_km",
        haversine_udf(
            F.col("latitude"),
            F.col("longitude"),
            F.col("station_lat"),
            F.col("station_long")
        )
    )

    # Write to disk to free memory
    cross = cross.checkpoint()

    # Keep only the nearest station per property
    from pyspark.sql.window import Window

    # For each property_id, order by ascending distance
    window =  Window.partitionBy("property_id").orderBy(F.col("distance_km").asc())

    # assign rank on each window partition
    cross = cross.withColumn("distance_rank", F.rank().over(window))

    # Keep only rank 1 (nearest station)
    nearest = cross.filter(F.col("distance_rank") == 1).drop("distance_rank")

    # logger.info("After keeping the nearest station: %s rows", f"{nearest.count():,}")

    return nearest

def add_proximity_buckets(df: DataFrame) -> DataFrame:
    """
    Categorize distances into easily readable buckets

    Instead of "0.347 km" we want something like "< 500m" - makes data easier to understand
    adding interpretive context without aggregating yet
    """

    df = df.withColumn(
        "transit_proximity_category",
        F.when(F.col("distance_km") <= 0.5, "< 500m")
         .when(F.col("distance_km") <= 1.0, "500m - 1km")
         .when(F.col("distance_km") <= 2.0, "1km - 2km")
         .when(F.col("distance_km") <= 5.0, "2km - 5km")
         .otherwise("> 5km")
    )

    return df

def write_silver(df: DataFrame) -> None:
    """
    create the dataframe for silver layer with the cross joined
    """

    OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

    logger.info("Writin enriched silver data to: %s", OUTPUT_PATH)

    (
        df
        .repartition(2)
        .write
        .mode("overwrite")
        .partitionBy("transit_proximity_category")
        .parquet(str(OUTPUT_PATH))
    )

    logger.info("Transit proximity data written to silver layer")

def main():
    spark = create_spark_session("TransitProximityJoin")

    logger.info("=== Transit Proximity Transformation ===")

    # Load data
    properties = spark.read.parquet(str(SILVER_PROPERTIES_PATH))
    stations = load_skytrain_stations(spark)

    # show files that will be used
    logger.info("Properties loaded: %s", f"{properties.count():,}")
    logger.info("Stations loaded: %s", f"{stations.count():,}")

    # Checkpoint for writing results to disk and preventing JVM from holding everything
    spark.sparkContext.setCheckpointDir("/app/data/checkpoints")

    # Run spatial join
    enriched = calculate_nearest_station(properties, stations)
    enriched = add_proximity_buckets(enriched)

    # preview results 
    # logger.info("Sample results - properties with nearest station:")
    # enriched.select(
    #     "property_id", "street_name", "neighbourhood_code",
    #     "current_land_value", "station_name", "distance_km",
    #     "transit_proximity_category" 
    # ).show(15, truncate=False)

    # # Summary starts by proximity category
    # logger.info("Property value by transit proximity: ")
    # enriched.groupBy("transit_proximity_category").agg(
    #     F.count("property_id").alias("property_count"),
    #     F.avg("current_land_value").alias("avg_land_value"),
    #     F.median("current_land_value").alias("median_land_value")
    # ).orderBy("transit_proximity_category").show()

    write_silver(enriched)

    spark.stop()
    logger.info("Transit proximity transformation complete!")

if __name__ == "__main__":
    main()