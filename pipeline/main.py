"""
pipeline/main.py
----------------
ETL Pipeline Orchestrator for the MTA Enterprise Workforce System.

Execution order:
  1. Extract   → load raw CSVs
  2. Validate  → flag and remove bad records
  3. Transform → compute hours, overtime, gross pay
  4. Load      → persist to PostgreSQL
  5. Export    → write processed CSVs to data/processed/
  6. BQ Upload → stream data to BigQuery (optional)

Usage:
  python -m pipeline.main
  python -m pipeline.main --skip-bq   (skip BigQuery step)
"""

import os
import sys
import time
import argparse
import pandas as pd
from datetime import datetime
from pipeline.logger import get_logger
from pipeline.extract import extract_employees, extract_time_logs
from pipeline.validate import validate_time_logs
from pipeline.transform import transform_time_logs, build_payroll_summary
from pipeline.load import load_employees, load_time_logs, load_payroll_summary

logger = get_logger("pipeline.main")

PROCESSED_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed")


def run_pipeline(skip_db: bool = False, skip_bq: bool = True) -> dict:
    """
    Runs the full ETL pipeline. Returns a summary dict with metrics.
    """
    start_time = time.time()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    logger.info("=" * 60)
    logger.info(f"  MTA Workforce ETL Pipeline — Run ID: {run_id}")
    logger.info("=" * 60)

    metrics = {
        "run_id": run_id,
        "started_at": datetime.utcnow().isoformat(),
        "steps": {}
    }

    # ── STEP 1: EXTRACT ──────────────────────────────────────────
    step_start = time.time()
    logger.info("\n[STEP 1/5] EXTRACT")
    try:
        employees_raw = extract_employees()
        logs_raw = extract_time_logs()
        metrics["steps"]["extract"] = {
            "employees_raw": len(employees_raw),
            "logs_raw": len(logs_raw),
            "duration_s": round(time.time() - step_start, 2)
        }
        logger.info(f"  Extract complete ({metrics['steps']['extract']['duration_s']}s)")
    except FileNotFoundError as e:
        logger.error(f"Extract failed: {e}")
        logger.error("  → Run: python data/generate_data.py  to generate mock data first")
        sys.exit(1)

    # ── STEP 2: VALIDATE ─────────────────────────────────────────
    step_start = time.time()
    logger.info("\n[STEP 2/5] VALIDATE")
    clean_logs, errors_df = validate_time_logs(logs_raw)
    metrics["steps"]["validate"] = {
        "clean_records": len(clean_logs),
        "flagged_records": len(errors_df),
        "error_rate_pct": round(len(errors_df) / len(logs_raw) * 100, 2),
        "duration_s": round(time.time() - step_start, 2)
    }
    logger.info(f"  Validate complete ({metrics['steps']['validate']['duration_s']}s)")

    # ── STEP 3: TRANSFORM ────────────────────────────────────────
    step_start = time.time()
    logger.info("\n[STEP 3/5] TRANSFORM")
    transformed_logs = transform_time_logs(clean_logs)
    payroll_summary = build_payroll_summary(transformed_logs, employees_raw)
    metrics["steps"]["transform"] = {
        "transformed_logs": len(transformed_logs),
        "payroll_summary_rows": len(payroll_summary),
        "total_hours": round(transformed_logs["hours_worked"].sum(), 2),
        "total_overtime_hours": round(transformed_logs["overtime_hours"].sum(), 2),
        "total_gross_pay": round(transformed_logs["gross_pay"].sum(), 2),
        "duration_s": round(time.time() - step_start, 2)
    }
    logger.info(f"  Transform complete ({metrics['steps']['transform']['duration_s']}s)")

    # ── EXPORT PROCESSED CSVs ─────────────────────────────────────
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    transformed_logs.to_csv(os.path.join(PROCESSED_DIR, "time_logs_processed.csv"), index=False)
    payroll_summary.to_csv(os.path.join(PROCESSED_DIR, "payroll_summary.csv"), index=False)
    employees_raw.to_csv(os.path.join(PROCESSED_DIR, "employees_processed.csv"), index=False)
    logger.info(f"  ✓ Processed CSVs written to: {PROCESSED_DIR}/")

    # ── STEP 4: LOAD (PostgreSQL) ─────────────────────────────────
    if not skip_db:
        step_start = time.time()
        logger.info("\n[STEP 4/5] LOAD → PostgreSQL")
        try:
            load_employees(employees_raw)
            load_time_logs(transformed_logs)
            load_payroll_summary(payroll_summary)
            metrics["steps"]["load"] = {
                "status": "success",
                "duration_s": round(time.time() - step_start, 2)
            }
            logger.info(f"  Load complete ({metrics['steps']['load']['duration_s']}s)")
        except Exception as e:
            logger.warning(f"  DB load skipped (no connection): {e}")
            metrics["steps"]["load"] = {"status": "skipped", "error": str(e)}
    else:
        logger.info("\n[STEP 4/5] LOAD → Skipped (--skip-db flag)")
        metrics["steps"]["load"] = {"status": "skipped"}

    # ── STEP 5: BIGQUERY UPLOAD ───────────────────────────────────
    if not skip_bq:
        step_start = time.time()
        logger.info("\n[STEP 5/5] BIGQUERY UPLOAD")
        try:
            from bigquery.bq_loader import upload_to_bigquery
            upload_to_bigquery(transformed_logs, payroll_summary, employees_raw)
            metrics["steps"]["bigquery"] = {
                "status": "success",
                "duration_s": round(time.time() - step_start, 2)
            }
        except Exception as e:
            logger.warning(f"  BigQuery upload skipped: {e}")
            metrics["steps"]["bigquery"] = {"status": "skipped", "error": str(e)}
    else:
        logger.info("\n[STEP 5/5] BIGQUERY → Skipped (--skip-bq flag)")
        metrics["steps"]["bigquery"] = {"status": "skipped"}

    # ── PIPELINE COMPLETE ─────────────────────────────────────────
    metrics["total_duration_s"] = round(time.time() - start_time, 2)
    metrics["status"] = "SUCCESS"

    logger.info("\n" + "=" * 60)
    logger.info(f"  PIPELINE COMPLETE IN {metrics['total_duration_s']}s")
    logger.info(f"  Records In   : {metrics['steps']['extract']['logs_raw']:,}")
    logger.info(f"  Records Clean: {metrics['steps']['validate']['clean_records']:,}")
    logger.info(f"  Flagged      : {metrics['steps']['validate']['flagged_records']:,} ({metrics['steps']['validate']['error_rate_pct']}%)")
    logger.info(f"  Total Hours  : {metrics['steps']['transform']['total_hours']:,.2f}h")
    logger.info(f"  Overtime Hrs : {metrics['steps']['transform']['total_overtime_hours']:,.2f}h")
    logger.info(f"  Total Payroll: ${metrics['steps']['transform']['total_gross_pay']:,.2f}")
    logger.info("=" * 60)

    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MTA Workforce ETL Pipeline")
    parser.add_argument("--skip-db", action="store_true", help="Skip PostgreSQL load step")
    parser.add_argument("--skip-bq", action="store_true", default=True, help="Skip BigQuery upload step")
    args = parser.parse_args()

    run_pipeline(skip_db=args.skip_db, skip_bq=args.skip_bq)
