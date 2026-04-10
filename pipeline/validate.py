"""
pipeline/validate.py
--------------------
Data validation rules for the MTA workforce ETL pipeline.

Rules enforced:
  1. Missing clock_out              → flagged as MISSING_CLOCK_OUT
  2. clock_out <= clock_in          → flagged as INVALID_CLOCK_SEQUENCE
  3. hours_worked < 0 or > 24      → flagged as INVALID_HOURS
  4. Exact duplicate log_id rows    → flagged as DUPLICATE_RECORD
  5. Missing employee_id            → flagged as MISSING_EMPLOYEE_ID

Returns:
  - clean_df   : valid records only
  - errors_df  : all flagged records with error reason
"""

import os
import pandas as pd
from datetime import datetime
from pipeline.logger import get_logger

logger = get_logger(__name__)

PROCESSED_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed")


def validate_time_logs(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Validates time log records, returns (clean_df, errors_df).
    """
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    errors = []

    original_count = len(df)
    logger.info(f"Starting validation on {original_count:,} records")

    # ── Rule 1: Remove exact duplicate log_id rows ──────────────
    duplicates_mask = df.duplicated(subset=["log_id"], keep="first")
    duplicate_rows = df[duplicates_mask].copy()
    duplicate_rows["error_reason"] = "DUPLICATE_RECORD"
    errors.append(duplicate_rows)

    df = df[~duplicates_mask].copy()
    logger.info(f"  [Rule 1] Removed {len(duplicate_rows):,} duplicate records")

    # ── Rule 2: Missing employee_id ─────────────────────────────
    missing_emp = df[df["employee_id"].isna() | (df["employee_id"] == "")].copy()
    missing_emp["error_reason"] = "MISSING_EMPLOYEE_ID"
    errors.append(missing_emp)
    df = df[~(df["employee_id"].isna() | (df["employee_id"] == ""))].copy()
    logger.info(f"  [Rule 2] Flagged {len(missing_emp):,} records with missing employee_id")

    # ── Rule 3: Missing clock_out ───────────────────────────────
    missing_out = df[df["clock_out"].isna()].copy()
    missing_out["error_reason"] = "MISSING_CLOCK_OUT"
    errors.append(missing_out)
    df = df[~df["clock_out"].isna()].copy()
    logger.info(f"  [Rule 3] Flagged {len(missing_out):,} records with missing clock_out")

    # ── Rule 4: clock_out <= clock_in (invalid sequence) ────────
    invalid_seq = df[df["clock_out"] <= df["clock_in"]].copy()
    invalid_seq["error_reason"] = "INVALID_CLOCK_SEQUENCE"
    errors.append(invalid_seq)
    df = df[df["clock_out"] > df["clock_in"]].copy()
    logger.info(f"  [Rule 4] Flagged {len(invalid_seq):,} records with invalid clock sequence")

    # ── Rule 5: Unrealistic hours (< 0.5h or > 24h) ─────────────
    df["_hours_check"] = (df["clock_out"] - df["clock_in"]).dt.total_seconds() / 3600
    invalid_hours = df[(df["_hours_check"] < 0.5) | (df["_hours_check"] > 24)].copy()
    invalid_hours["error_reason"] = "INVALID_HOURS"
    errors.append(invalid_hours)
    df = df[(df["_hours_check"] >= 0.5) & (df["_hours_check"] <= 24)].copy()
    df.drop(columns=["_hours_check"], inplace=True)
    logger.info(f"  [Rule 5] Flagged {len(invalid_hours):,} records with invalid hours")

    # ── Build errors DataFrame ───────────────────────────────────
    errors_df = pd.concat(errors, ignore_index=True) if errors else pd.DataFrame()
    errors_df["flagged_at"] = datetime.utcnow().isoformat()

    # ── Write validation report ──────────────────────────────────
    errors_path = os.path.join(PROCESSED_DIR, "validation_errors.csv")
    if not errors_df.empty:
        errors_df.to_csv(errors_path, index=False)
        logger.info(f"  Validation errors written → {errors_path}")

    total_errors = len(errors_df)
    logger.info(
        f"Validation complete: {original_count:,} in → "
        f"{len(df):,} clean | {total_errors:,} flagged "
        f"({total_errors / original_count * 100:.1f}% error rate)"
    )

    return df, errors_df
