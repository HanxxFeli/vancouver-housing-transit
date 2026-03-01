"""
transform_properties.py

Purpose: Read raw property tax data from bronze layer
clean it then write clean data to silver layer

Rules: 
- Fix data types (everything is a string from CSV)
- remove or handle null/missing values
- standardize column names (using snake_case)
- filter rows
- no aggregation (for gold layer work)

Running: 
docker compose exec pipeline python -m transformation.transform_properties
"""

import logging 
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

from transformation.spark_session import create_spark_session

logger = logging.getLogger(__name__)

#----CONFIG----
BRONZE_PATH: Path = (
    Path(__file__).parent.parent.parent / "data" / "bronze" / "property_tax" / "property_tax_raw.parquet"
)

SILVER_PATH: Path = Path(__file__).parent.parent.parent / "data" / "silver" / "properties_cleaned"

#----Transformation Functions----

def read_bronze(spark: SparkSession) -> DataFrame: 
    """
    read the raw property tax parquet file into a Spark DataFrame

    Args:
        spark (SparkSession): active SparkSession

    Returns:
        DataFrame: raw bronze data
    """
    logger.info("Reading bronze data from: %s", BRONZE_PATH)

    # read bronze parquet file into df
    df: DataFrame = spark.read.parquet(str(BRONZE_PATH))
    logger.info("Raw row count: %s | Columns: %d", f"{df.count():,}", len(df.columns))

    return df

def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Standardizes column names to snake_case

    Args:
        df (DataFrame): raw bronze dataframe with original column names

    Returns:
        DataFrame: Same data but renamed column names
    """

    rename_map: dict[str, str] = { 
        "PID": "property_id",
        "LEGAL_TYPE": "legal_type",
        "FOLIO": "folio",
        "LAND_COORDINATE": "land_coordinate",
        "ZONING_DISTRICT": "zoning_district",
        "ZONING_CLASSIFICATION": "zoning_classification",
        "LOT": "lot",
        "PLAN": "plan",
        "BLOCK": "block",
        "DISTRICT_LOT": "district_lot",
        "FROM_CIVIC_NUMBER": "street_number_from",
        "TO_CIVIC_NUMBER": "street_number_to",
        "STREET_NAME": "street_name",
        "PROPERTY_POSTAL_CODE": "postal_code",
        "NARRATIVE_LEGAL_LINE1": "legal_description",
        "NEIGHBOURHOOD_CODE": "neighbourhood_code",
        "REPORT_YEAR": "report_year",
        "CURRENT_LAND_VALUE": "current_land_value",
        "CURRENT_IMPROVEMENT_VALUE": "current_improvement_value",
        "TAX_ASSESSMENT_YEAR": "tax_year",
        "PREVIOUS_LAND_VALUE": "previous_land_value",
        "PREVIOUS_IMPROVEMENT_VALUE": "previous_improvement_value",
        "YEAR_BUILT": "year_built",
        "BIG_IMPROVEMENT_YEAR": "big_improvement_year",
        "TAX_LEVY": "tax_levy",
    }

    for old_name, new_name in rename_map.items():
        if old_name in df.columns:
            df = df.withColumnRenamed(old_name, new_name)

    return df

def cast_data_types(df: DataFrame) -> DataFrame:
    """
    Converts columns to their correct data types

    Args:
        df (DataFrame): df with string-typed numeric columns

    Returns:
        DataFrame: same data with correct types for each column
    """

    numeric_double_cols: list[str] = [
        "current_land_value",
        "current_improvement_value",
        "previous_land_value",
        "previous_improvement_value",
        "tax_levy",
    ]

    numeric_int_cols: list[str] = ["year_built", "report_year", "tax_year"]

    for col_name in numeric_double_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(DoubleType()))

    for col_name in numeric_int_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(IntegerType()))

    return df

def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Creates new columns calculated from existing data

    Args:
        df (DataFrame): Cleaned df with correct types 

    Returns:
        DataFrame: data with derived business metrics
    """

    # total assessed value = land + improvements (buildings)
    df = df.withColumn(
        "total_assessed_value",
        F.col("current_land_value") + F.col("current_improvement_value"),
    )

    # year over year land value change (absolute)
    df = df.withColumn(
        "land_value_change",
        F.col("current_land_value") - F.col("previous_land_value"),
    )

    # year over year change as percentage 
    df = df.withColumn(
        "land_value_change_pct",
        F.when(
            F.col("previous_land_value").isNotNull() & (F.col("previous_land_value") > 0),
            F.col("land_value_change") / F.col("previous_land_value") * 100,
        ).otherwise(None)
    )

    # property age in years 
    df = df.withColumn(
        "property_age_years",
        F.when(
            F.col("year_built").isNotNull() & (F.col("year_built") > 1800),
            F.lit(2024) - F.col("year_built"),
        ).otherwise(None)
    )

    return df

def filter_and_clean(df: DataFrame) -> DataFrame: 
    """
    removes unsuable rows and logs how many were dropped

    Args:
        df (DataFrame): DF before filtering

    Returns:
        DataFrame: Filtered DF with only valid rows
    """

    # get df initial row count 
    initial_count: int = df.count()

    df = df.filter(F.col("current_land_value").isNotNull())
    df = df.filter(F.col("current_land_value") > 0)

    # $500M+ land values are probably data errors
    df = df.filter(F.col("current_land_value") < 500_000_000)

    # df count after filtering
    final_count: int = df.count()

    logger.info(
        "Filtered %s rows (%s → %s)",
        f"{initial_count - final_count:,}",
        f"{initial_count:,}",
        f"{final_count:,}",
    )
    return df

def write_silver(df: DataFrame) -> None: 
    """
    Writes the cleaned df to the silver layer as a partitioned parquet

    Args:
        df (DataFrame): final cleaned df 

    Partition by neighbourhood_code
    """

    SILVER_PATH.mkdir(parents=True, exist_ok=True)
    logger.info("Writing silver data  to: %s", SILVER_PATH)


    (
        df.repartition(4)
        .write.mode("overwrite")
        .partitionBy("neighbourhood_code")
        .parquet(str(SILVER_PATH))
    )

    logger.info("Silver layer written successfully")

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )

    spark: SparkSession = create_spark_session("PropertyTransformation")

    logger.info("=== Property Tax Transformation ===")

    # call all the functions 
    df: DataFrame = (
        read_bronze(spark)
        .transform(clean_column_names)
        .transform(cast_data_types)
        .transform(add_derived_columns)
        .transform(filter_and_clean)
    )

    # show and verify important columns
    df.select(
        "property_id", "street_name", "neighbourhood_code",
        "current_land_value", "total_assessed_value", "year_built",
    ).show(10, truncate=False)

    write_silver(df)

    spark.stop()
    logger.info("Property Transformation complete!")


if __name__ == "__main__":
    main()