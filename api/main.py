"""
api/main.py
-----------
FastAPI application entry point for the MTA Enterprise Workforce API.

Endpoints:
  GET /health           — health check
  GET /employees        — employee listing
  GET /payroll          — payroll summaries
  GET /overtime-report  — overtime analytics
  GET /validation-errors — ETL data quality flags

Deployment:
  Local  : uvicorn api.main:app --reload
  Vercel : handler = Mangum(app)
"""

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from mangum import Mangum
from dotenv import load_dotenv

from api.routes import employees, payroll, overtime, validation
from api.database import check_db_connection

load_dotenv()

# ──────────────────────────────────────────────
# APP INIT
# ──────────────────────────────────────────────
app = FastAPI(
    title="MTA Enterprise Workforce API",
    description=(
        "Production-grade REST API for the MTA Workforce Data Pipeline & Analytics System. "
        "Exposes employee timekeeping data, payroll analytics, overtime reports, and ETL validation results."
    ),
    version="1.0.0",
    contact={"name": "MTA Data Engineering", "email": "data-engineering@mta-corp.org"},
    license_info={"name": "MIT"},
    docs_url="/docs",
    redoc_url="/redoc",
)

# ──────────────────────────────────────────────
# CORS MIDDLEWARE
# ──────────────────────────────────────────────
cors_origins = os.getenv("CORS_ORIGINS", "*").split(",")
app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# ──────────────────────────────────────────────
# ROUTERS
# ──────────────────────────────────────────────
app.include_router(employees.router)
app.include_router(payroll.router)
app.include_router(overtime.router)
app.include_router(validation.router)


# ──────────────────────────────────────────────
# ROOT + HEALTH
# ──────────────────────────────────────────────
@app.get("/", include_in_schema=False)
def root():
    return {
        "name": "MTA Enterprise Workforce API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health", tags=["System"], summary="API health check")
def health_check():
    """
    Returns API health status and database connectivity.
    Used by deployment platforms (Render / Railway) for readiness probes.
    """
    db_ok = check_db_connection()
    status = "healthy" if db_ok else "degraded"
    return JSONResponse(
        status_code=200 if db_ok else 503,
        content={
            "status": status,
            "database": "connected" if db_ok else "unreachable",
            "version": "1.0.0",
            "environment": os.getenv("ENVIRONMENT", "development"),
        }
    )


# ──────────────────────────────────────────────
# VERCEL / AWS LAMBDA HANDLER
# Mangum wraps FastAPI as an ASGI handler for serverless deployment
# ──────────────────────────────────────────────
handler = Mangum(app, lifespan="off")
