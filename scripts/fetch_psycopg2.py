"""
scripts/fetch_psycopg2.py
────────────────────────────────────────────────────────────────────────────────
Approach 1 — LEGACY:  psycopg2 + SQLAlchemy 2.0

  psycopg2 is the most widely-used PostgreSQL driver for Python.
  SQLAlchemy wraps it to give us a unified engine interface.
  pd.read_sql() accepts a SQLAlchemy engine directly.

Usage (standalone):
    python scripts/fetch_psycopg2.py

Returns a DataFrame when imported as a module:
    from scripts.fetch_psycopg2 import fetch_sales
    df = fetch_sales()
────────────────────────────────────────────────────────────────────────────────
"""

import logging
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import get_psycopg2_url

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Public API ────────────────────────────────────────────────────────────────
def fetch_sales(query: str = "SELECT * FROM sales ORDER BY sale_id") -> pd.DataFrame:
    """
    Fetch data from PostgreSQL into a Pandas DataFrame using psycopg2.

    Parameters
    ----------
    query : str
        Any valid SELECT statement.

    Returns
    -------
    pd.DataFrame
        Query results; empty DataFrame on failure.
    """
    engine = None
    try:
        engine = create_engine(get_psycopg2_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        log.info("[psycopg2] Fetched %d rows, %d columns.", len(df), len(df.columns))
        return df
    except Exception as exc:
        log.error("[psycopg2] Query failed: %s", exc)
        return pd.DataFrame()
    finally:
        if engine:
            engine.dispose()


def fetch_joined(
    query: str = """
        SELECT s.sale_id, s.customer_name, c.city, c.segment,
               s.product, s.quantity, s.price, s.sale_date
        FROM   sales s
        JOIN   customers c USING (customer_name)
        ORDER  BY s.sale_id
    """
) -> pd.DataFrame:
    """Fetch a JOIN between sales and customers."""
    return fetch_sales(query=query)


# ── Standalone execution ──────────────────────────────────────────────────────
if __name__ == "__main__":
    df = fetch_sales()
    if not df.empty:
        print(df.to_string(index=False))
