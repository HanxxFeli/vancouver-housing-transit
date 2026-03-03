"""
spark_session.py

Purpose: Create and configures a reusable SparkSession
Read data, run transformations and write output script.
"""

import logging

from pyspark.sql import SparkSession

# logger name
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "VancouverHousingTransit") -> SparkSession:
    """
    Creates a configured SparkSession for local development

    Args:
        app_name (str, optional): name in Spark UI. Defaults to "VancouverHousingTransit".

    Returns:
        SparkSession: configured session for use
    """
    spark: SparkSession = (
        SparkSession.builder.appName(app_name)
        # run locally, use all cores
        .master("local[*]")
        # memory allocated to each executor (worker process)
        .config("spark.executor.memory", "2g")
        # memory for the driver (coordinator process)
        .config("spark.driver.memory", "4g")
        # Number of partitions after shuffle
        # lower = fewer, larger files. 8 for local dev
        .config("spark.sql.shuffle.partitions", "8").getOrCreate()
    )

    # reduce noisy Spark output (can also be info)
    spark.sparkContext.setLogLevel("WARN")

    logger.info("SparkSession created: %s (Spark %s)", app_name, spark.version)

    return spark
