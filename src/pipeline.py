"""
pipeline.py

PURPOSE: The master orchestrator — runs every stage of the pipeline
         in the correct order, with structured logging and error handling.
"""

import argparse
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

# Configure root logger — all child loggers (logger = logging.getLogger(__name__))
# inherit this configuration automatically
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

sys.path.append(str(Path(__file__).parent))

from ingestion.ingest_property_tax import ingest_property_tax
from ingestion.ingest_translink import ingest_translink_stops
from transformation.transform_gold import main as transform_gold
from transformation.transform_properties import main as transform_properties
from transformation.transform_transit_proximity import \
    main as transform_proximity


def run_stage(
    stage_name: str, stage_func: Callable, skip_on_failure: bool = False
) -> bool:
    """ "
    Runs a single pipeline stage with timing and error handling

    Args:
        stage_name: name of the process shown in logs
        stage_func: callable to execute
        skip_on_failure: if True: continue the pipeline even if stage fails
                         if False (defaul), failure stops the entire pipeline

    Returns:
        bool: True if the stage succeeded, False if it failed and was skipped.
    """

    logger.info("=" * 50)
    logger.info("Starting stage: %s", stage_name)
    start_time: float = time.time()

    try:
        stage_func()  # runs the function (ingestion, transformation)
        elapsed: float = time.time() - start_time
        logger.info("Stage complete: %s (%.1fs)", stage_name, elapsed)
        return True

    except Exception as e:
        elapsed: float = time.time() - start_time
        logger.error("Stage FAILED: %s (%.1fs) — %s", stage_name, elapsed, e)

        if skip_on_failure:
            logger.warning("Continuing pipeline despite failure (skip_on_failure=True)")
            return False
        raise


# ----MAIN PIPELINE----
def run_pipeline(skip_ingestion: bool = False, stages: list = None) -> None:
    """
    Runs the full Bronze -> Silver -> Gold pipeline end to end.

    Args:
        skip_ingestion (bool, optional): If True, skip data download and use existing bronze files.
    """

    pipeline_start: float = time.time()
    logger.info("Starting Vancouver Housing-Transit Pipeline")
    logger.info("Run time: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # Determine which stages to run
    if stages is None:
        stages_to_run = ["ingest", "transform", "gold"]
        logger.info(f"Running all stages: {stages_to_run}")
    else:
        stages_to_run = stages
        logger.info(f"Running specific stages: {stages_to_run}")

    results: dict[str, bool] = {}

    # ----Bronze Layer----
    # general ingestion using "ingest" but doesnt match dag file
    if "ingest" in stages_to_run and not skip_ingestion:
        results["ingest_property_tax"] = run_stage(
            "Bronze: Ingest Property Tax Data", ingest_property_tax
        )
        results["ingest_translink"] = run_stage(
            "Bronze: Ingest TransLink GTFS Data", ingest_translink_stops
        )
    elif "ingest" in stages_to_run and skip_ingestion:
        logger.info("Skipping ingestion (--skip-ingestion flag set)")
    else:
        logger.info("Skipping ingestion (not in stages list)")

    # property ingestion
    if "ingest_properties" in stages_to_run:
        if skip_ingestion:
            logger.info("Skipping ingest_properties (--skip-ingestion flag set)")
        else:
            results["ingest_property_tax"] = run_stage(
                "Bronze: Ingest Property Tax Data", ingest_property_tax
            )

    # translink ingestion
    if "ingest_translink" in stages_to_run:
        if skip_ingestion:
            logger.info("Skipping ingest_translink (--skip-ingestion flag set)")
        else:
            results["ingest_translink"] = run_stage(
                "Bronze: Ingest TransLink GTFS Data", ingest_translink_stops
            )

    # ----Silver Layer----
    if "transform" in stages_to_run:
        results["transform_properties"] = run_stage(
            "Silver: Transform Property Data", transform_properties
        )
        results["transform_proximity"] = run_stage(
            "Silver: Calculate Transit Proximity", transform_proximity
        )
    else:
        logger.info("Skipping transformations (not in stages list)")

    # ----Gold Layer----
    if "gold" in stages_to_run:
        results["transform_gold"] = run_stage(
            "Gold: Build Neighbourhood Summaries", transform_gold
        )
    else:
        logger.info("Skipping gold layer (not in stages list)")

    # ----Summary----
    total_elapsed: float = time.time() - pipeline_start
    logger.info("=" * 50)
    logger.info("PIPELINE COMPLETE — total time: %.1fs", total_elapsed)

    for stage, success in results.items():
        logger.info("  %s %s", "✓" if success else "X", stage)

    failed: list[str] = []

    for stage_name, stage_succeeded in results.items():
        if not stage_succeeded:
            failed.append(stage_name)

    if failed:
        logger.warning("%d stage(s) had issues: %s", len(failed), failed)
    else:
        logger.info("All stages completed successfully")


# ----ENTRY----
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Vancouver Housing-Transit Pipeline")

    # Skip Ingestion Argument
    parser.add_argument(
        "--skip-ingestion",  # flag name
        action="store_true",  # if flag is present, set to True
        help="Skip data download and use existing bronze files",
    )

    # Stages argument
    parser.add_argument(
        "--stages",
        nargs="+",  # Accept one or more values
        choices=[
            "ingest",
            "ingest_properties",
            "ingest_translink",
            "transform",
            "gold",
        ],  # Only allow these values
        help="Which stages to run: ingest, transform, gold (can specify multiple)",
    )

    args = parser.parse_args()
    run_pipeline(skip_ingestion=args.skip_ingestion, stages=args.stages)
