"""
scripts/db_setup.py
────────────────────────────────────────────────────────────────────────────────
Responsibility:
  1. Connect to the default 'postgres' database.
  2. Create 'sales_analysis_db' if it does not exist.
  3. Connect to the new database and create:
       - sales      (main fact table)
       - customers  (dimension table for JOIN demos)
  4. Insert realistic sample data.
  5. Log every action; raise on unrecoverable errors.

Run once before any fetch scripts:
    python scripts/db_setup.py
────────────────────────────────────────────────────────────────────────────────
"""

import logging
import sys
import os

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# ── Make sure project root is on PYTHONPATH so config.py is importable ────────
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import DB_CONFIG

# ── Logging setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── SQL definitions ───────────────────────────────────────────────────────────
CREATE_CUSTOMERS = """
CREATE TABLE IF NOT EXISTS customers (
    customer_id   SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL UNIQUE,
    city          VARCHAR(100),
    segment       VARCHAR(50)
);
"""

CREATE_SALES = """
CREATE TABLE IF NOT EXISTS sales (
    sale_id       SERIAL PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    product       VARCHAR(100) NOT NULL,
    quantity      INT          NOT NULL CHECK (quantity > 0),
    price         NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    sale_date     DATE         NOT NULL
);
"""

SAMPLE_CUSTOMERS = """
INSERT INTO customers (customer_name, city, segment) VALUES
    ('Ali Khan',      'Lahore',     'Retail'),
    ('Sara Malik',    'Karachi',    'Wholesale'),
    ('Ahmed Raza',    'Islamabad',  'Retail'),
    ('Fatima Noor',   'Lahore',     'Corporate'),
    ('Usman Tariq',   'Faisalabad', 'Retail'),
    ('Zara Hussain',  'Multan',     'Wholesale'),
    ('Bilal Sheikh',  'Rawalpindi', 'Corporate'),
    ('Hina Baig',     'Sialkot',    'Retail')
ON CONFLICT (customer_name) DO NOTHING;
"""

SAMPLE_SALES = """
INSERT INTO sales (customer_name, product, quantity, price, sale_date) VALUES
    ('Ali Khan',     'Sugar',      50,  1200.00, '2026-03-01'),
    ('Sara Malik',   'Rice',       30,   900.00, '2026-03-02'),
    ('Ahmed Raza',   'Wheat Flour',80,  2400.00, '2026-03-03'),
    ('Fatima Noor',  'Cooking Oil',20,  3800.00, '2026-03-04'),
    ('Usman Tariq',  'Sugar',      45,  1080.00, '2026-03-05'),
    ('Zara Hussain', 'Rice',       60,  1800.00, '2026-03-06'),
    ('Bilal Sheikh', 'Tea',        15,   750.00, '2026-03-07'),
    ('Hina Baig',    'Wheat Flour',35,  1050.00, '2026-03-08'),
    ('Ali Khan',     'Cooking Oil',10,  1900.00, '2026-03-09'),
    ('Sara Malik',   'Tea',        25,  1250.00, '2026-03-10');
"""


# ── Helpers ───────────────────────────────────────────────────────────────────
def _connect_default() -> psycopg2.extensions.connection:
    """Connect to the default 'postgres' database (to create new databases)."""
    cfg = {**DB_CONFIG, "database": "postgres"}
    return psycopg2.connect(**cfg)


def _connect_app() -> psycopg2.extensions.connection:
    """Connect to sales_analysis_db."""
    return psycopg2.connect(**DB_CONFIG)


def create_database() -> None:
    """Create sales_analysis_db if it does not already exist."""
    conn = _connect_default()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (DB_CONFIG["database"],),
            )
            if cur.fetchone():
                log.info("Database '%s' already exists — skipping creation.", DB_CONFIG["database"])
            else:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(DB_CONFIG["database"])
                    )
                )
                log.info("Database '%s' created successfully.", DB_CONFIG["database"])
    finally:
        conn.close()


def create_tables() -> None:
    """Create customers and sales tables inside sales_analysis_db."""
    conn = _connect_app()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_CUSTOMERS)
            log.info("Table 'customers' ensured.")
            cur.execute(CREATE_SALES)
            log.info("Table 'sales' ensured.")
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Failed to create tables — rolled back.")
        raise
    finally:
        conn.close()


def insert_sample_data() -> None:
    """Populate tables with sample rows (idempotent for customers via ON CONFLICT)."""
    conn = _connect_app()
    try:
        with conn.cursor() as cur:
            cur.execute(SAMPLE_CUSTOMERS)
            log.info("Sample customers inserted / skipped duplicates.")
            cur.execute(SAMPLE_SALES)
            log.info("Sample sales inserted.")
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("Failed to insert sample data — rolled back.")
        raise
    finally:
        conn.close()


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    log.info("=== Starting database setup ===")
    create_database()
    create_tables()
    insert_sample_data()
    log.info("=== Database setup complete ===")


if __name__ == "__main__":
    main()
