"""
api/database.py
---------------
SQLAlchemy database connection setup for the FastAPI backend.
Uses psycopg2 with connection pooling for production efficiency.
Handles Supabase special-character passwords correctly via URL.create().
"""

import os
from urllib.parse import urlparse, quote
from sqlalchemy import create_engine, text, URL
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError
from dotenv import load_dotenv
import logging

load_dotenv()
logger = logging.getLogger(__name__)

_RAW_URL = os.getenv("DATABASE_URL", "postgresql://postgres:password@localhost:5432/mta_workforce")


def _build_engine():
    """
    Builds the SQLAlchemy engine safely handling special characters
    in passwords (e.g. /, @, %) by parsing the DATABASE_URL and using
    SQLAlchemy's URL.create() which accepts unencoded passwords.
    """
    try:
        parsed = urlparse(_RAW_URL.replace("%2F", "/").replace("%40", "@"))
        db_url = URL.create(
            drivername="postgresql+psycopg2",
            username=parsed.username,
            password=parsed.password,        # SQLAlchemy handles encoding internally
            host=parsed.hostname,
            port=parsed.port or 5432,
            database=parsed.path.lstrip("/"),
        )
        connect_args = {"connect_timeout": 10}
        # Supabase requires SSL; add if not a local connection
        if "supabase" in (parsed.hostname or ""):
            connect_args["sslmode"] = "require"
        return create_engine(
            db_url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=1800,
            echo=False,
            connect_args=connect_args,
        )
    except Exception as e:
        logger.warning(f"URL.create() failed ({e}), falling back to raw URL")
        return create_engine(
            _RAW_URL,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=1800,
            echo=False,
        )


engine = _build_engine()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db() -> Session:
    """FastAPI dependency: yields a DB session, closes after request."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_db_connection() -> bool:
    """Health check: verify DB is reachable."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except OperationalError as e:
        logger.error(f"DB connection failed: {e}")
        return False
