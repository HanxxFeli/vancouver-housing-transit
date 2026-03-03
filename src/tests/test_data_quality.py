"""
test_data_quality.py

Data quality tests — assertions that our pipeline output meets minimum standards.
"""

import pytest 
from pathlib import Path

import pandas as pd


GOLD_DIR = Path(__file__).parent.parent.parent / "data" / "gold"
SILVER_DIR = Path(__file__).parent.parent.parent / "data" / "silver"

class TestDataQuality:
    
    def test_silver_properties_exist(self):
        """Silver properties folder should exist after pipeline runs."""
        silver_path = SILVER_DIR / "properties_cleaned"
        assert silver_path.exists(), f"Silver properties directory not found: {silver_path}"
    
    def test_silver_properties_not_empty(self):
        """Silver properties should have at least one Parquet file."""
        silver_path = SILVER_DIR / "properties_cleaned"
        if not silver_path.exists():
            pytest.skip("Silver data doesn't exist yet — run the pipeline first")
        
        parquet_files = list(silver_path.rglob("*.parquet"))
        assert len(parquet_files) > 0, "No Parquet files found in silver properties"
    
    def test_gold_exists(self):
        """Gold layer should exist after pipeline runs."""
        gold_path = GOLD_DIR / "neighbourhood_summary"
        assert gold_path.exists(), f"Gold directory not found: {gold_path}"
    
    def test_no_negative_land_values(self):
        """Property land values should never be negative — a data quality failure."""
        silver_path = SILVER_DIR / "properties_cleaned"
        if not silver_path.exists():
            pytest.skip("Silver data doesn't exist yet")
        
        # Read one partition to check (faster than reading all)
        parquet_files = list(silver_path.rglob("*.parquet"))
        if not parquet_files:
            pytest.skip("No parquet files found")
        
        df = pd.read_parquet(parquet_files[0])
        
        if "current_land_value" in df.columns:
            negative_count = (df["current_land_value"] < 0).sum()
            assert negative_count == 0, f"Found {negative_count} properties with negative land values"
    
    def test_distance_values_reasonable(self):
        """All distances should be between 0 and 100km (Vancouver is not that big)."""
        silver_path = SILVER_DIR / "properties_with_transit"
        if not silver_path.exists():
            pytest.skip("Transit proximity data doesn't exist yet")
        
        parquet_files = list(silver_path.rglob("*.parquet"))
        if not parquet_files:
            pytest.skip("No parquet files found")
        
        df = pd.read_parquet(parquet_files[0])
        
        if "distance_km" in df.columns:
            valid_distances = df["distance_km"].dropna() # drop NAN before checking
            max_distance = df["distance_km"].max()
            assert max_distance < 100, f"Unreasonably large distance found: {max_distance}km"
            
            min_distance = df["distance_km"].min()
            assert min_distance >= 0, f"Negative distance found: {min_distance}km"