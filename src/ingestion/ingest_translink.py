"""
ingest_translink.py

Download Translink GTFS data to extract stop locations for bronze layer saving 
"""

import io
import logging
import os
import zipfile
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# load env variables from .env file
# allow TRANSLINK_API_KEY and ENVIRONMENT available as os.environ var
load_dotenv()

# logger name
logger = logging.getLogger(__name__)

# ----Config----
GTFS_URL: str = "https://gtfs-static.translink.ca/gtfs/google_transit.zip"

# Directory Output
OUTPUT_DIR: Path = (
    Path(__file__).parent.parent.parent / "data" / "bronze" / "translink_stops"
)


def ingest_translink_stops() -> Path:
    """
    Downloads Vancouver Translink data (zip), extracts stops.txt and saves
    it as parquet to bronze layer

    Returns:
        Path: path to the saved stops parquet file
    """

    logger.info("Starting TransLink GTFS ingestion")

    # create output directory if doesnt exist
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Download GTFS zip
    logger.info("Downloading GTFS feed from Translink...")

    response = requests.get(GTFS_URL, timeout=120)

    # throw error if server returned failure
    response.raise_for_status()

    response_size_mb: float = len(response.content) / 1024 / 1024
    logger.info("Download Complete. Response size: %.2f MB", response_size_mb)

    # ----Extract stops.txt----
    logger.info("Extracting stops.txt from GTFS zip...")

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        logger.debug("Files in GTFS zip: %s", zf.namelist())

        # open stops.txt and read into df
        with zf.open("stops.txt") as stops_file:
            stops_df: pd.DataFrame = pd.read_csv(stops_file)

    logger.info("Loaded %s transit stops", f"{len(stops_df):,}")
    logger.debug("Stop columns: %s", list(stops_df.columns))

    # Save stops as Parquet
    output_path: Path = OUTPUT_DIR / "translink_stops_raw.parquet"
    stops_df.to_parquet(
        output_path, engine="pyarrow", compression="snappy", index=False
    )
    logger.info("Saved stops Parquet: %s (%d rows)", output_path, len(stops_df))

    # Filter to SkyTrain stations only
    # SkyTrain have stop names containing "Station"
    skytrain_df: pd.DataFrame = stops_df[
        stops_df["stop_name"].str.contains("Station", na=False)
    ]

    skytrain_path: Path = OUTPUT_DIR / "skytrain_stations_raw.parquet"
    skytrain_df.to_parquet(
        skytrain_path, engine="pyarrow", compression="snappy", index=False
    )

    logger.info(
        "Saved SkyTrain stations Parquet: %s (%d stations)",
        skytrain_path,
        len(skytrain_df),
    )

    return output_path


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    ingest_translink_stops()
    logger.info("Translink Ingestion complete!")
