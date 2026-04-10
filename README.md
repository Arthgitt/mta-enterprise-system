# MTA Enterprise Workforce Data Pipeline & Analytics System

![Python](https://img.shields.io/badge/Python-3.12-blue?logo=python)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?logo=fastapi)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![BigQuery](https://img.shields.io/badge/BigQuery-Google-4285F4?logo=googlecloud)
![C#](https://img.shields.io/badge/C%23-.NET_8-512BD4?logo=dotnet)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![License](https://img.shields.io/badge/License-MIT-green)

> **Production-grade enterprise workforce timekeeping and payroll analytics platform.**  
> Inspired by systems like Kronos/ADP — processes 5,000+ employee time records daily through a full ETL pipeline, exposes a REST API, and surfaces insights in a real-time analytics dashboard.

---

## Architecture

```
Raw CSV Data (5,200+ logs)
        │
        ▼
┌──────────────────┐
│  Python ETL       │  extract → validate → transform → load
│  pipeline/        │  (5 validation rules, OT calculation)
└──────┬───────────┘
       │
       ├──────────────────────────────────┐
       ▼                                  ▼
┌──────────────┐                 ┌─────────────────┐
│  PostgreSQL  │                 │  Google BigQuery │
│  (live OLTP) │                 │  (analytics DWH) │
└──────┬───────┘                 └─────────────────┘
       │
       ▼
┌──────────────────┐       ┌──────────────────────┐
│  FastAPI Backend  │◄─────│  APScheduler (cron)   │
│  (6 endpoints)    │       │  ETL @ 2AM daily      │
└──────┬────────────┘       └──────────────────────┘
       │
       ├─────────────────────────────────┐
       ▼                                 ▼
┌──────────────────┐           ┌──────────────────────┐
│  HTML/JS         │           │  C# Admin Console     │
│  Dashboard       │           │  (API consumer)        │
│  (Chart.js)      │           │  (.NET 8)             │
└──────────────────┘           └──────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data Generation | Python (pandas, random) |
| ETL Pipeline | Python 3.12, pandas 2.2 |
| Database | PostgreSQL 15 (SQLAlchemy) |
| Analytics Warehouse | Google BigQuery |
| REST API | FastAPI 0.111, Pydantic v2 |
| Scheduling | APScheduler 3.10 |
| Admin Dashboard | C# (.NET 8) console app |
| Frontend Dashboard | HTML5, Chart.js 4, Vanilla JS |
| Containerization | Docker + Docker Compose |
| Deployment | Vercel (API + static dashboard) |

---

## Project Structure

```
mta-enterprise-system/
├── data/
│   ├── raw/                    # Raw CSV input (employees.csv, time_logs.csv)
│   ├── processed/              # ETL output (cleaned data, validation errors)
│   └── generate_data.py        # Mock dataset generator (1000 employees, 5200+ logs)
├── pipeline/
│   ├── extract.py              # Step 1: Read raw CSVs
│   ├── validate.py             # Step 2: 5-rule validation engine
│   ├── transform.py            # Step 3: Compute hours, overtime, gross pay
│   ├── load.py                 # Step 4: Upsert into PostgreSQL
│   ├── logger.py               # Structured rotating-file logging
│   └── main.py                 # ETL orchestrator CLI
├── database/
│   ├── schema.sql              # 4 tables: employees, time_logs, payroll_summary, validation_errors
│   └── queries.sql             # 8 advanced SQL queries (CTEs, window functions, RANK, LAG)
├── api/
│   ├── main.py                 # FastAPI app + Mangum handler (Vercel)
│   ├── database.py             # SQLAlchemy engine + session
│   ├── models.py               # Pydantic response models
│   └── routes/
│       ├── employees.py        # GET /employees (paginated, filterable)
│       ├── payroll.py          # GET /payroll, /department-summary, /weekly-trend
│       ├── overtime.py         # GET /overtime-report, /employees
│       └── validation.py       # GET /validation-errors, /summary
├── bigquery/
│   ├── bq_loader.py            # Upload DataFrames to BigQuery
│   └── bq_queries.sql          # 4 BigQuery analytics queries
├── scheduler/
│   ├── cron_jobs.py            # APScheduler: ETL @ 2AM, reports @ 6AM UTC
│   └── tasks.py                # Scheduled task functions
├── csharp/WorkforceAdmin/
│   ├── Program.cs              # Console admin menu (6 views, business logic)
│   ├── ApiClient.cs            # HttpClient API wrapper (async, typed)
│   ├── Models.cs               # C# records matching API JSON shapes
│   └── WorkforceAdmin.csproj  # .NET 8 console app
├── dashboard/
│   ├── index.html              # Analytics dashboard
│   ├── style.css               # Dark glassmorphism theme
│   └── app.js                  # Chart.js + API data fetching
├── logs/                       # ETL logs (rotating, 10MB max)
├── Dockerfile
├── docker-compose.yml
├── vercel.json
├── requirements.txt
└── .env.example
```

---

## Quick Start (Local)

### Prerequisites
- Python 3.12+
- PostgreSQL 15+ (or use Docker Compose)
- .NET 8 SDK (for C# component)
- Node.js (optional, only if you want a static server for the dashboard)

### 1. Clone & Setup

```bash
git clone https://github.com/YOUR_USERNAME/mta-enterprise-system.git
cd mta-enterprise-system

# Create virtual environment
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Copy and configure environment variables
cp .env.example .env
# Edit .env with your DATABASE_URL
```

### 2. Generate Mock Data

```bash
python data/generate_data.py
# Creates: data/raw/employees.csv (1000 rows)
#          data/raw/time_logs.csv (5200+ rows with bad data)
```

### 3. Set Up Database

```bash
# Option A: Docker (recommended)
docker-compose up postgres -d

# Option B: Local PostgreSQL
psql -U postgres -c "CREATE DATABASE mta_workforce;"
psql -U postgres -d mta_workforce -f database/schema.sql
```

### 4. Run ETL Pipeline

```bash
# Full run (loads to PostgreSQL)
python -m pipeline.main

# Skip database (CSV output only)
python -m pipeline.main --skip-db

# Output:
# data/processed/time_logs_processed.csv
# data/processed/payroll_summary.csv
# data/processed/validation_errors.csv
# logs/etl.log
```

### 5. Start the API

```bash
uvicorn api.main:app --reload --port 8000

# API Docs:      http://localhost:8000/docs
# Health Check:  http://localhost:8000/health
```

### 6. Open the Dashboard

```bash
# Open directly in browser:
open dashboard/index.html

# Or serve with Python:
python -m http.server 5500 --directory dashboard
# Then visit: http://localhost:5500
```

### 7. Run C# Admin Console

```bash
cd csharp/WorkforceAdmin
dotnet run
# Or with custom API URL:
dotnet run -- --api-url http://localhost:8000
```

---

## API Documentation

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/health` | API + DB health check |
| `GET` | `/employees` | Paginated employee list (filters: department, status) |
| `GET` | `/employees/{id}` | Single employee + payroll stats |
| `GET` | `/payroll` | Paginated payroll summaries |
| `GET` | `/payroll/department-summary` | Aggregated payroll by department |
| `GET` | `/payroll/weekly-trend` | Weekly payroll + cumulative running total |
| `GET` | `/overtime-report` | OT hours/cost by department with rate % |
| `GET` | `/overtime-report/employees` | Top OT earners (RANK() window function) |
| `GET` | `/validation-errors` | ETL validation flags (filterable by type) |
| `GET` | `/validation-errors/summary` | Error counts grouped by type |

**Interactive docs**: `/docs` (Swagger UI) · `/redoc` (ReDoc)

---

## SQL Highlights

The `database/queries.sql` contains 8 production-grade queries:

| # | Query | SQL Features |
|---|---|---|
| Q1 | Total hours per employee | JOIN, GROUP BY, aggregate |
| Q2 | Overtime by department | HAVING filter |
| Q3 | Payroll cost by dept + month | DATE_TRUNC, NULLIF |
| Q4 | Rank employees by hours | `RANK()`, `DENSE_RANK()`, `NTILE()` OVER |
| Q5 | Running total payroll | `SUM() OVER (ORDER BY)` cumulative |
| Q6 | Overtime % by department | CASE WHEN, percentage math |
| Q7 | Top 10 costliest employees | CTE (`WITH ... AS`), subquery |
| Q8 | Attendance streak (gap detection) | `LAG()` OVER PARTITION BY |

---

## Deployment (Vercel)

Vercel is recommended for free hosting of the FastAPI backend + static dashboard.

### 1. Supabase PostgreSQL (free tier)
1. Create account at [supabase.com](https://supabase.com)
2. Create new project → copy the **Connection String** (Transaction mode, port 6543)
3. Run schema: Supabase SQL Editor → paste `database/schema.sql`

### 2. Deploy to Vercel

```bash
npm install -g vercel
vercel login
vercel deploy

# Set environment variable:
vercel env add DATABASE_URL
# Paste your Supabase connection string
```

### 3. Vercel Environment Variables

| Variable | Description |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string (Supabase) |
| `ENVIRONMENT` | `production` |
| `CORS_ORIGINS` | `*` or your frontend domain |

> **Note:** Vercel serverless functions have a 10-second timeout (free tier).  
> For the scheduler (daily cron jobs), use Railway or Render for background workers.

### Docker Compose (self-hosted)

```bash
# Setup
cp .env.example .env          # configure DATABASE_URL

# Start all services
docker-compose up --build -d

# Run ETL pipeline
docker-compose exec api python -m pipeline.main

# View logs
docker-compose logs -f api
docker-compose logs -f scheduler
```

---

## BigQuery Integration (optional)

1. Create a GCP project and enable BigQuery API
2. Create a service account → download JSON key
3. Set env vars:

```bash
BIGQUERY_PROJECT_ID=your-project-id
BIGQUERY_DATASET_ID=mta_workforce_analytics
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```

4. Run pipeline with BQ upload:
```bash
python -m pipeline.main --no-skip-bq
```

---

## Scheduling

The `scheduler/cron_jobs.py` uses APScheduler to run on a schedule:

| Job | Schedule | Description |
|---|---|---|
| `run_etl_pipeline` | Daily @ 02:00 UTC | Full extract → validate → transform → load |
| `run_report_generation` | Daily @ 06:00 UTC | Text report written to `logs/daily_report_YYYY-MM-DD.txt` |

```bash
# Run the scheduler
python -m scheduler.cron_jobs
```



---

## License

MIT — free to use, modify, and deploy for portfolio/commercial purposes.
