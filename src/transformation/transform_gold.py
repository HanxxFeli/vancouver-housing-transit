"""
transform gold.py 

PURPOSE: Aggregate silver data into neighbourhood-level summaries
that answers: "How does transit proximity affect property values in 
Vancouver neighbourhoods?"

Gold Layer Rules:
- Aggregated - one row per neighbourhood
- Directly answerable - column names should be self-explanatory
- Optimized for reading - structured for the API
"""

from itertools import chain
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

import sys
import os
import shutil
import logging

sys.path.append(str(Path(__file__).parent.parent))
from transformation.spark_session import create_spark_session

logger = logging.getLogger(__name__)

# ----Config----
BASE_DATA_PATH = Path(os.environ.get("BASE_DATA_PATH", "/app/data"))

SILVER_PATH = BASE_DATA_PATH / "silver" / "properties_with_transit"
GOLD_PATH = BASE_DATA_PATH / "gold" / "neighbourhood_summary"

# Mapping of numeric neighbourhood codes to human-readable names
NEIGHBOURHOOD_NAMES: dict[str, str] = {
    "1": "West Point Grey",
    "2": "Kitsilano",
    "3": "Arbutus Ridge",
    "4": "Dunbar-Southlands",
    "5": "Oakridge",
    "6": "Marpole",
    "7": "Fairview",
    "8": "Shaughnessy",
    "9": "Kerrisdale",
    "10": "Sunset",
    "11": "Cambie",
    "12": "Victoria-Fraserview",
    "13": "Mount Pleasant",
    "14": "Grandview-Woodland",
    "15": "Riley Park",
    "16": "Kensington-Cedar Cottage",
    "17": "Killarney",
    "18": "Victoria-Fraserview",
    "19": "Renfrew-Collingwood",
    "20": "Sunset",
    "21": "Hastings-Sunrise",
    "22": "Renfrew-Collingwood",
    "23": "Kensington-Cedar Cottage",
    "24": "Killarney",
    "25": "South Cambie",
    "26": "Downtown",
    "27": "West End",
    "28": "West End",
    "29": "Downtown",
    "30": "Downtown",
}


def add_neighbourhood_names(df: DataFrame) -> DataFrame:
    """
    Adds a human-readable neighbourhood_name column based on numeric neighbourhood_code.
    Uses a Spark map expression to avoid UDFs.
    """
    mapping_expr = F.create_map([F.lit(x) for x in chain(*NEIGHBOURHOOD_NAMES.items())])
    return df.withColumn(
        "neighbourhood_name", mapping_expr[F.col("neighbourhood_code").cast("string")]
    )


def build_neighbourhood_summary(df: DataFrame) -> DataFrame:
    """
    Aggregates property data to neighbourhood level

    gets the answer to the business question:
    - for each neighbourhood, what are the average property values?
    - does proximity to SkyTrain correlate with higher values?
    - which neighbourhoods have the most/least transit coverage?
    """

    neighbourhood_transit_summary = (
        df.groupBy("neighbourhood_code", "transit_proximity_category")
        .agg(
            F.count("property_id").alias("property_count"),
            # Average values
            F.round(F.avg("current_land_value"), 0).alias("avg_land_value"),
            F.round(F.avg("total_assessed_value"), 0).alias("avg_total_assessed_value"),
            # Median is more robust than mean for property values (less skewed by outliers)
            F.round(F.percentile_approx("current_land_value", 0.5), 0).alias(
                "median_land_value"
            ),
            # Range (min and max give a sense of how diverse the neighbourhood is)
            F.round(F.min("current_land_value"), 0).alias("min_land_value"),
            F.round(F.max("current_land_value"), 0).alias("max_land_value"),
            # Average distance to nearest station
            F.round(F.avg("distance_km"), 3).alias("avg_distance_to_station_km"),
            # Most common nearest station
            F.first("station_name", ignorenulls=True).alias("primary_station"),
            # Year over year change (average)
            F.round(F.avg("land_value_change_pct"), 2).alias("avg_yoy_change_pct"),
        )
        .orderBy("neighbourhood_code", "transit_proximity_category")
    )

    return neighbourhood_transit_summary


def build_neighbourhood_overview(df: DataFrame) -> DataFrame:
    """
    Creates a single-row-per-neighbourhood overview
    One point per neighbourhood with key stats
    """

    overview = (
        df.groupBy("neighbourhood_code")
        .agg(
            F.count("property_id").alias("total_properties"),
            F.round(F.avg("current_land_value"), 0).alias("avg_land_value"),
            F.round(F.avg("distance_km"), 3).alias("avg_distance_to_station_km"),
            # Percentage of properties within 500m of a station
            F.round(
                F.sum(
                    F.when(
                        F.col("transit_proximity_category") == "< 500m", 1
                    ).otherwise(0)
                )
                / F.count("property_id")
                * 100,
                1,
            ).alias("pct_within_500m_station"),
            F.round(F.avg("land_value_change_pct"), 2).alias("avg_yoy_change_pct"),
        )
        .orderBy(F.col("avg_land_value").desc())
    )

    return overview


def write_gold(neighbourhood_transit: DataFrame, overview: DataFrame) -> None:
    GOLD_PATH.mkdir(parents=True, exist_ok=True)

    detail_path = GOLD_PATH / "neighbourhood_transit_detail"
    overview_path = GOLD_PATH / "neighbourhood_overview"

    # Remove existing directories before writing to avoid permission conflicts
    if detail_path.exists():
        shutil.rmtree(detail_path)
    if overview_path.exists():
        shutil.rmtree(overview_path)

    # Write detailed summary
    (
        neighbourhood_transit.repartition(1)
        .write.mode("overwrite")
        .parquet(str(detail_path))
    )

    # Write overview
    (overview.repartition(1).write.mode("overwrite").parquet(str(overview_path)))


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    spark = create_spark_session("GoldLayerAggregation")

    logger.info("===Gold Layer Aggregation")

    silver_df = spark.read.parquet(str(SILVER_PATH))
    logger.info("Loaded %s silver records", f"{silver_df.count():,}")

    # Build gold tables
    neighbourhood_transit = add_neighbourhood_names(
        build_neighbourhood_summary(silver_df)
    )
    overview = add_neighbourhood_names(build_neighbourhood_overview(silver_df))

    # Preview
    logger.info("Top 10 neighbourhoods by average land value:")
    overview.show(10, truncate=False)

    logger.info("Sample Detail - Kitsilano by transit proximity:")
    neighbourhood_transit.filter(F.col("neighbourhood_name") == "Kitsilano").show(
        truncate=False
    )

    write_gold(neighbourhood_transit, overview)

    spark.stop()
    logger.info("Gold Layer Complete")


if __name__ == "__main__":
    main()
