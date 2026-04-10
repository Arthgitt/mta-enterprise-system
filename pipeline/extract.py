"""
pipeline/extract.py
-------------------
Extraction layer: reads raw CSV files from data/raw/ and returns DataFrames.
"""

import os
import pandas as pd
from pipeline.logger import get_logger

logger = get_logger(__name__)

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")


def extract_employees() -> pd.DataFrame:
    """Load raw employees CSV into a DataFrame."""
    path = os.path.join(RAW_DIR, "employees.csv")
    if not os.path.exists(path):
        logger.error(f"Employees file not found: {path}")
        raise FileNotFoundError(f"Missing file: {path}")
    
    df = pd.read_csv(path, dtype={"employee_id": str})
    logger.info(f"Extracted {len(df):,} employee records from {path}")
    return df


def extract_time_logs() -> pd.DataFrame:
    """
    Load raw time_logs CSV into a DataFrame.
    Parses clock_in as datetime; clock_out may be empty (bad data).
    """
    path = os.path.join(RAW_DIR, "time_logs.csv")
    if not os.path.exists(path):
        logger.error(f"Time logs file not found: {path}")
        raise FileNotFoundError(f"Missing file: {path}")

    df = pd.read_csv(
        path,
        dtype={"employee_id": str, "log_id": str},
        parse_dates=["clock_in"],
        keep_default_na=True
    )
    # clock_out parsed separately to allow empty strings
    df["clock_out"] = pd.to_datetime(df["clock_out"], errors="coerce")

    logger.info(f"Extracted {len(df):,} time log records from {path}")
    return df
