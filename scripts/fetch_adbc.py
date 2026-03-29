"""
scripts/fetch_adbc.py
────────────────────────────────────────────────────────────────────────────────
Approach 3 — CUTTING-EDGE:  ADBC + PyArrow

  ADBC (Arrow Database Connectivity) is the modern successor to JDBC/ODBC for
  columnar data systems.  It transfers data directly in Apache Arrow format,
  eliminating Python object overhead and enabling zero-copy reads.

  Why ADBC is faster than psycopg* for analytics:
    - No row-by-row Python object construction
    - Data stays in contiguous memory buffers (Arrow columnar)
    - pd.DataFrame() from Arrow is O(1) for numeric columns (zero-copy)

Usage (standalone):
    python scripts/fetch_adbc.py

Note:
  ADBC does not integrate with SQLAlchemy — it uses its own connection API.
────────────────────────────────────────────────────────────────────────────────
"""

import logging
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import get_adbc_uri

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def fetch_sales(query: str = "SELECT * FROM sales ORDER BY sale_id") -> pd.DataFrame:
    """
    Fetch data using ADBC PostgreSQL driver with PyArrow backend.

    The result passes through an Arrow RecordBatch and is converted to
    a Pandas DataFrame — numeric columns are zero-copy on modern hardware.

    Parameters
    ----------
    query : str
        Any valid SELECT statement.

    Returns
    -------
    pd.DataFrame
    """
    try:
        import adbc_driver_postgresql.dbapi as pg_adbc
    except ImportError:
        log.error(
            "[ADBC] adbc-driver-postgresql not installed. "
            "Run: pip install adbc-driver-postgresql"
        )
        return pd.DataFrame()

    conn = None
    try:
        conn = pg_adbc.connect(get_adbc_uri())
        with conn.cursor() as cur:
            cur.execute(query)
            arrow_table = cur.fetch_arrow_table()
            df = arrow_table.to_pandas()
        log.info("[ADBC] Fetched %d rows, %d columns via Arrow.", len(df), len(df.columns))
        return df
    except Exception as exc:
        log.error("[ADBC] Query failed: %s", exc)
        return pd.DataFrame()
    finally:
        if conn:
            conn.close()


def fetch_to_parquet(
    output_path: str = "data/sales.parquet",
    query: str = "SELECT * FROM sales ORDER BY sale_id",
) -> None:
    """
    Fetch from PostgreSQL and persist directly to Parquet via PyArrow.
    Parquet files are ideal for downstream big-data analytics (Spark, DuckDB).
    """
    try:
        import adbc_driver_postgresql.dbapi as pg_adbc
        import pyarrow.parquet as pq
    except ImportError as e:
        log.error("[ADBC] Missing dependency: %s", e)
        return

    conn = None
    try:
        conn = pg_adbc.connect(get_adbc_uri())
        with conn.cursor() as cur:
            cur.execute(query)
            arrow_table = cur.fetch_arrow_table()

        os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
        pq.write_table(arrow_table, output_path, compression="snappy")
        log.info("[ADBC] Saved %d rows to '%s'.", arrow_table.num_rows, output_path)
    except Exception as exc:
        log.error("[ADBC] Failed to write Parquet: %s", exc)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    df = fetch_sales()
    if not df.empty:
        print(df.to_string(index=False))
