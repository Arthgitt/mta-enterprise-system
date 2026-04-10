"""
scheduler/cron_jobs.py
----------------------
APScheduler cron configuration for the MTA Workforce Pipeline.

Schedule:
  - ETL Pipeline    : daily at 02:00 AM UTC
  - Report Generation: daily at 06:00 AM UTC

Usage:
  python -m scheduler.cron_jobs          (runs indefinitely)
  docker-compose up scheduler            (production)

The scheduler runs in a separate process / container from the API.
"""

import logging
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from scheduler.tasks import run_etl_pipeline, run_report_generation

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger("scheduler")


def job_listener(event):
    """Logs success or failure for each scheduled job execution."""
    if event.exception:
        logger.error(f"JOB FAILED: {event.job_id} — {event.exception}")
    else:
        logger.info(f"JOB SUCCESS: {event.job_id} executed successfully")


def start_scheduler():
    """
    Initializes and starts the APScheduler blocking scheduler.
    Runs indefinitely until SIGTERM / Ctrl+C.
    """
    scheduler = BlockingScheduler(timezone="UTC")
    scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    # ── Job 1: Daily ETL Pipeline at 02:00 UTC ──────────────────
    scheduler.add_job(
        run_etl_pipeline,
        trigger=CronTrigger(hour=2, minute=0),
        id="etl_pipeline",
        name="MTA ETL Pipeline — Daily",
        replace_existing=True,
        max_instances=1,          # Prevent overlapping runs
        coalesce=True             # Run once even if multiple triggers missed
    )

    # ── Job 2: Daily Report Generation at 06:00 UTC ─────────────
    scheduler.add_job(
        run_report_generation,
        trigger=CronTrigger(hour=6, minute=0),
        id="report_generation",
        name="MTA Daily Report — 6AM",
        replace_existing=True,
        max_instances=1,
        coalesce=True
    )

    logger.info("=" * 50)
    logger.info("  MTA Workforce Scheduler — Started")
    logger.info("  ETL Pipeline    : daily @ 02:00 UTC")
    logger.info("  Report Gen      : daily @ 06:00 UTC")
    logger.info("=" * 50)

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped gracefully.")
        scheduler.shutdown()


if __name__ == "__main__":
    start_scheduler()
