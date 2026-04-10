"""
api/routes/validation.py
------------------------
GET /validation-errors — return all records flagged by validation rules
GET /validation-errors/summary — breakdown of errors by type
"""

import os
import csv
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/validation-errors", tags=["Validation"])

PROCESSED_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "data", "processed"
)
VALIDATION_FILE = os.path.join(PROCESSED_DIR, "validation_errors.csv")


def _read_validation_errors() -> list[dict]:
    """Reads validation_errors.csv into a list of dicts."""
    if not os.path.exists(VALIDATION_FILE):
        return []
    with open(VALIDATION_FILE, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@router.get("", summary="Get all validation errors from last ETL run")
def get_validation_errors(
    error_type: str = Query(None, description="Filter by error reason"),
    limit:      int = Query(100, ge=1, le=1000)
):
    """
    Returns records flagged during ETL validation.
    Error types: DUPLICATE_RECORD | MISSING_CLOCK_OUT | INVALID_CLOCK_SEQUENCE |
                 INVALID_HOURS | MISSING_EMPLOYEE_ID
    """
    errors = _read_validation_errors()

    if error_type:
        errors = [e for e in errors if e.get("error_reason") == error_type.upper()]

    return {
        "total": len(errors),
        "showing": min(len(errors), limit),
        "errors": errors[:limit]
    }


@router.get("/summary", summary="Error breakdown by type")
def get_validation_summary():
    """Returns counts and percentages for each error type."""
    errors = _read_validation_errors()
    if not errors:
        return {"total_errors": 0, "breakdown": []}

    total = len(errors)
    counts: dict = {}
    for e in errors:
        reason = e.get("error_reason", "UNKNOWN")
        counts[reason] = counts.get(reason, 0) + 1

    breakdown = [
        {
            "error_reason": reason,
            "count": count,
            "percentage": round(count / total * 100, 2)
        }
        for reason, count in sorted(counts.items(), key=lambda x: -x[1])
    ]

    return {"total_errors": total, "breakdown": breakdown}
