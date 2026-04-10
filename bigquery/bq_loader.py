"""
bigquery/bq_loader.py
---------------------
Uploads processed workforce data to Google BigQuery for large-scale analytics.

Prerequisites:
  1. Set BIGQUERY_PROJECT_ID in .env
  2. Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON path
  3. Run: pip install google-cloud-bigquery

Usage:
  Called automatically by pipeline/main.py when --skip-bq is not set.
  Can also be run standalone: python -m bigquery.bq_loader
"""

import os
import pandas as pd
from datetime import datetime
from pipeline.logger import get_logger

logger = get_logger("bigquery.bq_loader")

PROJECT_ID  = os.getenv("BIGQUERY_PROJECT_ID", "your-gcp-project-id")
DATASET_ID  = os.getenv("BIGQUERY_DATASET_ID", "mta_workforce_analytics")


def _get_client():
    """Returns a BigQuery client (lazy import so BQ is optional)."""
    try:
        from google.cloud import bigquery
        return bigquery.Client(project=PROJECT_ID)
    except ImportError:
        raise ImportError(
            "google-cloud-bigquery not installed. "
            "Run: pip install google-cloud-bigquery"
        )


def _ensure_dataset(client) -> None:
    """Creates the BigQuery dataset if it doesn't already exist."""
    from google.cloud import bigquery
    dataset_ref = client.dataset(DATASET_ID)
    try:
        client.get_dataset(dataset_ref)
        logger.debug(f"Dataset {DATASET_ID} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created BigQuery dataset: {DATASET_ID}")


def upload_dataframe(df: pd.DataFrame, table_name: str, write_mode: str = "WRITE_APPEND") -> int:
    """
    Uploads a pandas DataFrame to a BigQuery table.

    Args:
        df         : DataFrame to upload
        table_name : Target table name (within DATASET_ID)
        write_mode : WRITE_APPEND | WRITE_TRUNCATE | WRITE_EMPTY
    Returns:
        Row count uploaded
    """
    from google.cloud import bigquery

    client = _get_client()
    _ensure_dataset(client)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    job_config = bigquery.LoadJobConfig(write_disposition=write_mode)

    # Ensure datetime columns are UTC-naive (BQ requirement)
    for col in df.select_dtypes(include=["datetime64[ns, UTC]", "datetimetz"]).columns:
        df[col] = df[col].dt.tz_localize(None)

    logger.info(f"Uploading {len(df):,} rows → BQ table: {table_ref}")
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for completion

    logger.info(f"  ✓ BQ upload complete: {table_name} ({len(df):,} rows)")
    return len(df)


def upload_to_bigquery(
    time_logs: pd.DataFrame,
    payroll_summary: pd.DataFrame,
    employees: pd.DataFrame
) -> dict:
    """
    Orchestrates upload of all three DataFrames to BigQuery.
    Called by pipeline/main.py after successful ETL run.
    """
    results = {}
    run_tag = datetime.utcnow().strftime("%Y%m%d")

    logger.info(f"Starting BigQuery upload (run: {run_tag})...")

    try:
        results["employees"]       = upload_dataframe(employees,         "employees",       "WRITE_TRUNCATE")
        results["time_logs"]       = upload_dataframe(time_logs,         "time_logs",       "WRITE_APPEND")
        results["payroll_summary"] = upload_dataframe(payroll_summary,   "payroll_summary", "WRITE_APPEND")
        logger.info("BigQuery upload complete ✓")
    except Exception as e:
        logger.error(f"BigQuery upload failed: {e}")
        results["error"] = str(e)

    return results


if __name__ == "__main__":
    # Standalone test: upload processed CSVs from data/processed/
    import os
    PROCESSED = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed")

    tl = pd.read_csv(os.path.join(PROCESSED, "time_logs_processed.csv"))
    ps = pd.read_csv(os.path.join(PROCESSED, "payroll_summary.csv"))
    emp = pd.read_csv(os.path.join(PROCESSED, "employees_processed.csv"))

    upload_to_bigquery(tl, ps, emp)
