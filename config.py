# =============================================================================
# config.py — Centralized Database Configuration
# =============================================================================
# Edit these values to match your local or cloud PostgreSQL instance.
# Never commit real credentials to version control.

DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "sales_analysis_db",
    "user":     "postgres",       # ← change to your PostgreSQL username
    "password": "1234",  # ← change to your PostgreSQL password
}

# Convenience: connection string templates (used by SQLAlchemy & ADBC)
def get_psycopg2_url() -> str:
    c = DB_CONFIG
    return f"postgresql+psycopg2://{c['user']}:{c['password']}@{c['host']}:{c['port']}/{c['database']}"

def get_psycopg3_url() -> str:
    c = DB_CONFIG
    return f"postgresql+psycopg://{c['user']}:{c['password']}@{c['host']}:{c['port']}/{c['database']}"

def get_adbc_uri() -> str:
    c = DB_CONFIG
    return f"postgresql://{c['user']}:{c['password']}@{c['host']}:{c['port']}/{c['database']}"
