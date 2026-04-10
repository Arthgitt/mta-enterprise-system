"""
pipeline/transform.py
---------------------
Transformation layer for the MTA workforce ETL pipeline.

Transforms:
  - Computes hours_worked from clock_in / clock_out
  - Computes overtime_hours (> 8 hours per shift)
  - Computes gross_pay (regular + overtime at 1.5x)
  - Fills missing department with "UNKNOWN"
  - Normalizes column types for DB insertion
"""

import pandas as pd
from pipeline.logger import get_logger

logger = get_logger(__name__)

STANDARD_HOURS_PER_SHIFT = 8.0   # hours before overtime kicks in
OVERTIME_MULTIPLIER = 1.5         # time-and-a-half


def transform_time_logs(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies all transformation rules to the clean time log DataFrame.
    Returns an enriched DataFrame with computed payroll columns.
    """
    logger.info(f"Starting transformation on {len(df):,} clean records")
    df = df.copy()

    # ── Compute hours_worked ─────────────────────────────────────
    df["hours_worked"] = (
        (df["clock_out"] - df["clock_in"]).dt.total_seconds() / 3600
    ).round(4)
    logger.debug("Computed hours_worked")

    # ── Compute overtime_hours ───────────────────────────────────
    df["overtime_hours"] = (df["hours_worked"] - STANDARD_HOURS_PER_SHIFT).clip(lower=0).round(4)
    logger.debug("Computed overtime_hours")

    # ── Compute regular_hours (capped at standard) ───────────────
    df["regular_hours"] = df["hours_worked"].clip(upper=STANDARD_HOURS_PER_SHIFT).round(4)

    # ── Compute gross_pay ────────────────────────────────────────
    df["regular_pay"] = (df["regular_hours"] * df["pay_rate"]).round(2)
    df["overtime_pay"] = (df["overtime_hours"] * df["pay_rate"] * OVERTIME_MULTIPLIER).round(2)
    df["gross_pay"] = (df["regular_pay"] + df["overtime_pay"]).round(2)
    logger.debug("Computed gross_pay (regular + overtime)")

    # ── Fill missing departments ─────────────────────────────────
    dept_filled = df["department"].isna().sum()
    df["department"] = df["department"].fillna("UNKNOWN")
    if dept_filled > 0:
        logger.info(f"  Filled {dept_filled} missing department values with 'UNKNOWN'")

    # ── Fix column types for DB ──────────────────────────────────
    df["clock_in"] = df["clock_in"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["clock_out"] = df["clock_out"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["log_date"] = pd.to_datetime(df["log_date"]).dt.strftime("%Y-%m-%d")

    logger.info(
        f"Transformation complete → "
        f"avg hours: {df['hours_worked'].mean():.2f}h | "
        f"avg gross_pay: ${df['gross_pay'].mean():.2f} | "
        f"total OT hours: {df['overtime_hours'].sum():,.1f}h"
    )
    return df


def build_payroll_summary(time_logs: pd.DataFrame, employees: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregates time_logs into a payroll_summary table keyed by employee + week.
    """
    logger.info("Building payroll summary...")

    df = time_logs.copy()
    df["log_date"] = pd.to_datetime(df["log_date"])
    df["week_start"] = df["log_date"] - pd.to_timedelta(df["log_date"].dt.dayofweek, unit="D")
    df["week_start"] = df["week_start"].dt.strftime("%Y-%m-%d")

    summary = df.groupby(["employee_id", "week_start", "department"], as_index=False).agg(
        total_hours=("hours_worked", "sum"),
        total_overtime=("overtime_hours", "sum"),
        total_gross_pay=("gross_pay", "sum"),
        num_shifts=("log_id", "count"),
    )
    summary["total_hours"] = summary["total_hours"].round(2)
    summary["total_overtime"] = summary["total_overtime"].round(2)
    summary["total_gross_pay"] = summary["total_gross_pay"].round(2)
    summary["pay_period"] = "weekly"

    logger.info(f"Payroll summary built: {len(summary):,} rows across {summary['week_start'].nunique()} weeks")
    return summary
