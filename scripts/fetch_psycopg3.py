"""
scripts/fetch_psycopg3.py
────────────────────────────────────────────────────────────────────────────────
Approach 2 — MODERN:  psycopg3 + SQLAlchemy 2.0

  psycopg3 (package name: psycopg) is the redesigned successor to psycopg2.
  It supports asyncio natively, binary protocol, and better type mapping.
  SQLAlchemy 2.0 dialect: "postgresql+psycopg://"

Usage (standalone):
    python scripts/fetch_psycopg3.py
────────────────────────────────────────────────────────────────────────────────
"""

import logging
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import get_psycopg3_url

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def fetch_sales(query: str = "SELECT * FROM sales ORDER BY sale_id") -> pd.DataFrame:
    """
    Fetch data using psycopg3 (psycopg) + SQLAlchemy.

    psycopg3 key improvements over psycopg2:
      - Binary protocol support (faster large payloads)
      - Native async support
      - Better Python type adapters

    Parameters
    ----------
    query : str
        Any valid SELECT statement.

    Returns
    -------
    pd.DataFrame
    """
    engine = None
    try:
        engine = create_engine(get_psycopg3_url(), pool_pre_ping=True)
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        log.info("[psycopg3] Fetched %d rows, %d columns.", len(df), len(df.columns))
        return df
    except Exception as exc:
        log.error("[psycopg3] Query failed: %s", exc)
        return pd.DataFrame()
    finally:
        if engine:
            engine.dispose()


def fetch_summary() -> pd.DataFrame:
    """Convenience: aggregate quantity sold per product."""
    query = """
        SELECT   product,
                 SUM(quantity)           AS total_qty,
                 ROUND(SUM(price), 2)    AS total_revenue,
                 COUNT(*)                AS num_sales
        FROM     sales
        GROUP BY product
        ORDER BY total_revenue DESC
    """
    return fetch_sales(query=query)


if __name__ == "__main__":
    df = fetch_sales()
    if not df.empty:
        print(df.to_string(index=False))
    print("\n--- Product Summary ---")
    print(fetch_summary().to_string(index=False))
