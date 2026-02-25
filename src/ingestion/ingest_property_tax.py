"""
Download Vancouver property tax data and save as a Parquet file 
for bronze layer
"""

import logging 
import os
from pathlib import Path 
from tqdm import tqdm

import pandas as pd
import requests
from dotenv import load_dotenv

# load env variables from .env file 
# allow TRANSLINK_API_KEY and ENVIRONMENT available as os.environ var
load_dotenv() 

# logger name 
logger = logging.getLogger(__name__)

# ----Config---- 
PROPERTY_TAX_URL: str = (
    "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/"
    "property-tax-report/exports/csv"
    "?lang=en&timezone=America%2FVancouver&use_labels=true&delimiter=%2C"
)

# Directory Output 
OUTPUT_DIR: Path = Path(__file__).parent.parent.parent / "data" / "bronze" / "property_tax"

def ingest_property_tax() -> Path: 
    """
    Downloads Vancouver property tax data and saves it as parquet to bronze layer

    Returns:
        Path: path to the saved parquet file
    """

    logger.info("Starting property tax ingestion")
    logger.info("Source URL: %s", PROPERTY_TAX_URL)

    # create output directory if doesnt exist 
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Download the data
    logger.info("Downloading data from Vancouver Open Data Portal")

    # download in chunks (stream=True)
    response = requests.get(PROPERTY_TAX_URL, stream=True, timeout=120)

    # throw error if server returned failure
    response.raise_for_status()

    temp_csv_path: Path = OUTPUT_DIR / "raw_download.csv"

    # check total file size
    total_size = int(response.headers.get("content-length", 0))

    with open(temp_csv_path, "wb") as f: 
        # download in 8kb chunks using iter_content
        with tqdm(
            total=total_size,
            unit="B",
            unit_scale=True,
            desc="Downloading"
        ) as progress:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
                progress.update(len(chunk))
    
    logger.info("Download complete. Saved to %s", temp_csv_path)

    #----Convert to Parquet----
    logger.info("Converting to Parquet format...")

    # low_memory=False for pandas to analyze the file to infer column types 
    df: pd.DataFrame = pd.read_csv(temp_csv_path, low_memory=False)

    logger.info("Loaded %s rows and %d columns",f"{len(df):,}", len(df.columns))
    logger.debug("Columns: %s", list(df.columns))

    output_path: Path = OUTPUT_DIR / "property_tax_raw.parquet"

    # conversion 
    df.to_parquet(output_path, engine="pyarrow", compression="snappy", index=False)

    # remove temporary CSV 
    temp_csv_path.unlink()

    file_size_mb: float = output_path.stat().st_size / 1024 / 1024
    logger.info("Saved Parquet file: %s (%.2f MB, %s rows)", output_path, file_size_mb, f"{len(df):,}")

    return output_path

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
    )
    ingest_property_tax()
    logger.info("Property Tax Ingestion Complete!")