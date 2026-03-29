# PostgreSQL → Pandas Pipeline

A minimal, production-ready Python project demonstrating **three approaches** to
connecting PostgreSQL with Pandas for data analysis.

---

## Project Structure

```
postgres_pandas_pipeline/
│
├── data/                        # Parquet / CSV outputs (auto-created)
├── notebooks/
│    └── main.ipynb              # Full analysis demo
├── scripts/
│    ├── db_setup.py             # Create DB, tables, and seed data
│    ├── fetch_psycopg2.py       # Approach 1: psycopg2 + SQLAlchemy
│    ├── fetch_psycopg3.py       # Approach 2: psycopg3 + SQLAlchemy
│    └── fetch_adbc.py           # Approach 3: ADBC + PyArrow
├── config.py                    # Centralised credentials
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configure credentials

Edit `config.py`:

```python
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "sales_analysis_db",
    "user":     "postgres",       # ← your username
    "password": "your_password",  # ← your password
}
```

### 3. Create the database and seed data

```bash
python scripts/db_setup.py
```

### 4. Test individual fetch scripts

```bash
python scripts/fetch_psycopg2.py
python scripts/fetch_psycopg3.py
python scripts/fetch_adbc.py
```

### 5. Open the notebook

```bash
cd notebooks
jupyter notebook main.ipynb
```

---

## Database Schema

### `sales`
| Column | Type | Notes |
|--------|------|-------|
| sale_id | SERIAL PK | Auto-increment |
| customer_name | VARCHAR(100) | NOT NULL |
| product | VARCHAR(100) | NOT NULL |
| quantity | INT | > 0 |
| price | NUMERIC(10,2) | ≥ 0 |
| sale_date | DATE | NOT NULL |

### `customers`
| Column | Type | Notes |
|--------|------|-------|
| customer_id | SERIAL PK | |
| customer_name | VARCHAR(100) | UNIQUE |
| city | VARCHAR(100) | |
| segment | VARCHAR(50) | Retail / Wholesale / Corporate |

---

## Driver Comparison

| | psycopg2 | psycopg3 | ADBC |
|--|----------|----------|------|
| Maturity | Very stable | Stable | New |
| Async | ✗ | ✓ | ✓ |
| Binary protocol | ✗ | ✓ | ✓ |
| Arrow output | ✗ | ✗ | ✓ |
| Best for | Legacy code | New projects | Analytics |

---

## Error Handling Strategy

- Every script wraps database calls in `try/except`.
- All log messages use Python's `logging` module (not `print`).
- Engines call `.dispose()` in `finally` blocks to release connections.
- `db_setup.py` uses `ON CONFLICT DO NOTHING` for idempotent inserts.

---

## Scaling to Production

| Local | Production |
|-------|-----------|
| `localhost` PostgreSQL | AWS RDS / Supabase / Neon |
| psycopg2 | psycopg3 (async) or ADBC |
| CSV / in-memory | Parquet / Arrow / DuckDB |
| Single script | Airflow / Prefect DAG |
# Database-to-DataFrame-PostgreSQL-Pandas-Data-Ingestion-Pipeline
