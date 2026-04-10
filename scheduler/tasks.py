"""
scheduler/tasks.py
------------------
Defines the scheduled task functions for the MTA workforce pipeline.
These functions are registered with APScheduler in cron_jobs.py.
"""

import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def run_etl_pipeline():
    """
    Scheduled task: runs the full ETL pipeline.
    Executes daily at 2 AM via APScheduler.
    """
    from pipeline.main import run_pipeline

    logger.info(f"[SCHEDULER] ETL pipeline triggered at {datetime.utcnow().isoformat()}")
    try:
        metrics = run_pipeline(skip_db=False, skip_bq=True)
        logger.info(
            f"[SCHEDULER] ETL complete — "
            f"{metrics['steps']['validate']['clean_records']:,} records processed "
            f"in {metrics['total_duration_s']}s"
        )
    except Exception as e:
        logger.error(f"[SCHEDULER] ETL pipeline FAILED: {e}", exc_info=True)


def run_report_generation():
    """
    Scheduled task: generates daily summary report.
    Executes daily at 6 AM via APScheduler.
    Reads processed data and writes a human-readable text report.
    """
    import pandas as pd
    from tabulate import tabulate

    logger.info(f"[SCHEDULER] Report generation triggered at {datetime.utcnow().isoformat()}")
    PROCESSED_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed")
    REPORTS_DIR   = os.path.join(os.path.dirname(os.path.dirname(__file__)), "logs")
    os.makedirs(REPORTS_DIR, exist_ok=True)

    report_date = datetime.utcnow().strftime("%Y-%m-%d")
    report_path = os.path.join(REPORTS_DIR, f"daily_report_{report_date}.txt")

    try:
        tl = pd.read_csv(os.path.join(PROCESSED_DIR, "time_logs_processed.csv"))
        ps = pd.read_csv(os.path.join(PROCESSED_DIR, "payroll_summary.csv"))

        # Department summary
        dept_summary = tl.groupby("department").agg(
            shifts=("log_id", "count"),
            total_hours=("hours_worked", "sum"),
            total_ot=("overtime_hours", "sum"),
            total_payroll=("gross_pay", "sum")
        ).round(2).sort_values("total_payroll", ascending=False).reset_index()

        report_lines = [
            "=" * 70,
            f"  MTA ENTERPRISE WORKFORCE — DAILY REPORT",
            f"  Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC",
            "=" * 70,
            "",
            f"SUMMARY",
            f"  Total Shifts    : {len(tl):,}",
            f"  Total Employees : {tl['employee_id'].nunique():,}",
            f"  Total Hours     : {tl['hours_worked'].sum():,.2f}h",
            f"  Overtime Hours  : {tl['overtime_hours'].sum():,.2f}h",
            f"  Total Payroll   : ${tl['gross_pay'].sum():,.2f}",
            "",
            "PAYROLL BY DEPARTMENT",
            tabulate(dept_summary, headers="keys", tablefmt="simple", showindex=False),
            ""
        ]

        with open(report_path, "w") as f:
            f.write("\n".join(report_lines))

        logger.info(f"[SCHEDULER] Daily report written → {report_path}")

    except Exception as e:
        logger.error(f"[SCHEDULER] Report generation FAILED: {e}", exc_info=True)
