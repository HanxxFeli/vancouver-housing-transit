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
from pyspark.sql.window import Window

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
    Joins at neighbourhood level instead of property level.
    
    Since properties only have neighbourhood codes (not individual coordinates),
    joining at the neighbourhood centroid level is both correct and efficient.
    22 neighbourhoods × 50 stations = 1,100 combinations vs 5 million.
    """
    
    # Build neighbourhood centroids as a small DataFrame
    neighbourhood_coords: dict[str, tuple[float, float]] = {
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
    
    # Convert to a small Spark DataFrame
    # This is tiny — 22 rows, no memory issues
    from pyspark.sql.types import StructType, StructField, StringType
    
    schema = StructType([
        StructField("neighbourhood_code", StringType(), True),
        StructField("neighbourhood_lat", DoubleType(), True),
        StructField("neighbourhood_lon", DoubleType(), True),
    ])
    
    neighbourhood_df: DataFrame = properties.sparkSession.createDataFrame(
        [(k, v[0], v[1]) for k, v in neighbourhood_coords.items()],
        schema=schema,
    )
    
    # Cross join neighbourhoods × stations (22 × 50 = 1,100 rows)
    cross: DataFrame = neighbourhood_df.crossJoin(stations)
    
    # Calculate distance for each neighbourhood-station pair
    cross = cross.withColumn(
        "distance_km",
        haversine_udf(
            F.col("neighbourhood_lat"),
            F.col("neighbourhood_lon"),
            F.col("station_lat"),
            F.col("station_long"),
        ),
    )

    # Find nearest station per neighbourhood
    window = Window.partitionBy("neighbourhood_code").orderBy(F.col("distance_km").asc())
    cross = cross.withColumn("distance_rank", F.rank().over(window))
    nearest_per_neighbourhood: DataFrame = cross.filter(
        F.col("distance_rank") == 1
    ).drop("distance_rank", "neighbourhood_lat", "neighbourhood_lon")
    
    # Join result back onto properties using neighbourhood_code
    # This is a simple join on a key — fast and memory-efficient
    enriched: DataFrame = properties.join(
        nearest_per_neighbourhood,
        on="neighbourhood_code",
        how="left",
    )
    
    logger.info("Enriched properties with nearest station per neighbourhood")
    return enriched

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

    logger.info("Writing enriched silver data to: %s", OUTPUT_PATH)

    (
        df
        .repartition(2)
        .write
        .mode("overwrite")
        .partitionBy("transit_proximity_category")
        .parquet(str(OUTPUT_PATH))
    )

    logger.info("Transit proximity data written to silver layer")

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    spark = create_spark_session("TransitProximityJoin")

    logger.info("=== Transit Proximity Transformation ===")

    # Load data
    properties: DataFrame = spark.read.parquet(str(SILVER_PROPERTIES_PATH))
    stations: DataFrame = load_skytrain_stations(spark)

    # show files that will be used
    logger.info("Properties loaded: %s", f"{properties.count():,}")
    logger.info("Stations loaded: %s", f"{stations.count():,}")

    # Run spatial join
    enriched: DataFrame = calculate_nearest_station(properties, stations)
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