"""
main.py — Self-Bootstrapping Project Entry Point (uv-powered)
================================================================================
Just run:
    python main.py

This single file will automatically:
  [1] Check if `uv` is installed — if not, install it for you
  [2] Create a virtual environment via `uv venv`
  [3] Install all dependencies via `uv pip install -r requirements.txt`
  [4] Re-launch itself inside the new venv (so imports work)
  [5] Create the PostgreSQL database, tables, and seed sample data
  [6] Fetch data using all 3 driver approaches (psycopg2, psycopg3, ADBC)
  [7] Run a benchmark comparing driver speeds
  [8] Launch the Jupyter notebook for interactive analysis

Optional flags (skip bootstrapping if venv already exists):
    python main.py --setup       → only DB setup
    python main.py --fetch       → only fetch data
    python main.py --benchmark   → only benchmark drivers
    python main.py --notebook    → only launch Jupyter
    python main.py --no-notebook → full pipeline but skip Jupyter at the end
================================================================================
"""

# ── Standard-library only in the bootstrap phase ─────────────────────────────
import argparse
import logging
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

# ── Constants ─────────────────────────────────────────────────────────────────
PROJECT_ROOT  = Path(__file__).parent.resolve()
VENV_DIR      = PROJECT_ROOT / ".venv"
REQUIREMENTS  = PROJECT_ROOT / "requirements.txt"
DIVIDER       = "=" * 70

# Path to the Python interpreter inside the uv-created venv
if platform.system() == "Windows":
    VENV_PYTHON = VENV_DIR / "Scripts" / "python.exe"
    VENV_JUPYTER = VENV_DIR / "Scripts" / "jupyter.exe"
else:
    VENV_PYTHON  = VENV_DIR / "bin" / "python"
    VENV_JUPYTER = VENV_DIR / "bin" / "jupyter"

# Sentinel env-var so we know we're already inside the venv re-launch
_BOOTSTRAPPED = "PG_PIPELINE_BOOTSTRAPPED"

# ── Logging (basic — upgraded after bootstrap) ────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ==============================================================================
# PHASE 1 — BOOTSTRAP  (runs with the system Python, stdlib only)
# ==============================================================================

def _run(cmd: list[str], check: bool = True, **kwargs) -> subprocess.CompletedProcess:
    """Run a subprocess, streaming output live."""
    log.info("  $ %s", " ".join(str(c) for c in cmd))
    return subprocess.run(cmd, check=check, **kwargs)


def ensure_uv() -> str:
    """
    Return the path to `uv`.  If uv is not on PATH, install it via the
    official installer script (curl/powershell).
    """
    uv_path = shutil.which("uv")
    if uv_path:
        log.info("✓ uv found: %s", uv_path)
        return uv_path

    log.info("uv not found — installing uv automatically ...")

    if platform.system() == "Windows":
        _run([
            "powershell", "-ExecutionPolicy", "ByPass", "-Command",
            "irm https://astral.sh/uv/install.ps1 | iex",
        ])
        # uv installs to %USERPROFILE%\.cargo\bin on Windows
        candidate = Path.home() / ".cargo" / "bin" / "uv.exe"
    else:
        _run(["sh", "-c", "curl -LsSf https://astral.sh/uv/install.sh | sh"])
        candidate = Path.home() / ".cargo" / "bin" / "uv"

    uv_path = shutil.which("uv") or (str(candidate) if candidate.exists() else None)
    if not uv_path:
        log.error("Could not locate uv after installation.")
        log.error("Install manually:  https://docs.astral.sh/uv/getting-started/installation/")
        sys.exit(1)

    log.info("✓ uv installed: %s", uv_path)
    return uv_path


def create_venv(uv: str) -> None:
    """Create .venv using `uv venv` (idempotent — skips if already exists)."""
    if VENV_PYTHON.exists():
        log.info("✓ Virtual environment already exists — skipping creation.")
        return
    log.info("Creating virtual environment with uv ...")
    _run([uv, "venv", str(VENV_DIR)])
    log.info("✓ Virtual environment created: %s", VENV_DIR)


def install_dependencies(uv: str) -> None:
    """Install packages from requirements.txt into the venv using uv pip."""
    log.info("Installing dependencies from requirements.txt ...")
    _run([uv, "pip", "install", "--python", str(VENV_PYTHON),
          "-r", str(REQUIREMENTS)])
    log.info("✓ All dependencies installed.")


def relaunch_in_venv() -> None:
    """
    Re-execute this same script using the venv Python so that all
    third-party imports (pandas, sqlalchemy, psycopg2 …) are available.
    Sets a sentinel env-var to avoid infinite re-launch loops.
    """
    log.info(DIVIDER)
    log.info("Re-launching inside virtual environment ...")
    log.info(DIVIDER)
    env = {**os.environ, _BOOTSTRAPPED: "1"}
    result = subprocess.run(
        [str(VENV_PYTHON), __file__] + sys.argv[1:],
        env=env,
    )
    sys.exit(result.returncode)


def bootstrap() -> None:
    """Full bootstrap: install uv → create venv → install deps → relaunch."""
    log.info(DIVIDER)
    log.info("  BOOTSTRAP — Setting up project environment")
    log.info(DIVIDER)
    uv = ensure_uv()
    create_venv(uv)
    install_dependencies(uv)
    relaunch_in_venv()   # ← does not return


# ==============================================================================
# PHASE 2 — PIPELINE  (runs inside the venv, all deps available)
# ==============================================================================

def step_setup() -> bool:
    """Step 1: Create database, tables, and seed sample data."""
    log.info(DIVIDER)
    log.info("STEP 1 — Database Setup (db_setup.py)")
    log.info(DIVIDER)
    try:
        from scripts.db_setup import main as db_main  # noqa: PLC0415
        db_main()
        return True
    except Exception as exc:
        log.error("Setup failed: %s", exc)
        log.error(
            "Make sure PostgreSQL is running and config.py has the correct "
            "username / password."
        )
        return False


def step_fetch() -> None:
    """Step 2: Fetch data with all three driver approaches."""
    log.info(DIVIDER)
    log.info("STEP 2 — Fetch Data (3 Approaches)")
    log.info(DIVIDER)

    # ── Approach 1: psycopg2 + SQLAlchemy (legacy) ────────────────────────
    log.info("[ 1/3 ] psycopg2 + SQLAlchemy  (legacy)")
    try:
        from scripts.fetch_psycopg2 import fetch_sales as fetch_v2  # noqa: PLC0415
        df = fetch_v2()
        if not df.empty:
            log.info("  ✓  %d rows fetched. Preview:\n%s",
                     len(df), df.head(3).to_string(index=False))
        else:
            log.warning("  ✗  No data returned — check DB seed step.")
    except Exception as exc:
        log.error("  psycopg2 error: %s", exc)

    print()

    # ── Approach 2: psycopg3 + SQLAlchemy (modern) ────────────────────────
    log.info("[ 2/3 ] psycopg3 + SQLAlchemy  (modern)")
    try:
        from scripts.fetch_psycopg3 import fetch_sales as fetch_v3, fetch_summary  # noqa: PLC0415
        df = fetch_v3()
        if not df.empty:
            log.info("  ✓  %d rows fetched.", len(df))
            log.info("  Product summary:\n%s", fetch_summary().to_string(index=False))
        else:
            log.warning("  ✗  No data returned.")
    except Exception as exc:
        log.error("  psycopg3 error: %s", exc)

    print()

    # ── Approach 3: ADBC + PyArrow (cutting-edge) ─────────────────────────
    log.info("[ 3/3 ] ADBC + PyArrow  (cutting-edge)")
    try:
        from scripts.fetch_adbc import fetch_sales as fetch_adbc, fetch_to_parquet  # noqa: PLC0415
        df = fetch_adbc()
        if not df.empty:
            log.info("  ✓  %d rows fetched.", len(df))
            fetch_to_parquet(output_path="data/sales_raw.parquet")
            log.info("  ✓  Saved to data/sales_raw.parquet")
        else:
            log.warning("  ✗  No data returned.")
    except Exception as exc:
        log.error("  ADBC error: %s", exc)


def step_benchmark() -> None:
    """Step 3: Time all three fetch approaches and print a comparison table."""
    log.info(DIVIDER)
    log.info("STEP 3 — Driver Speed Benchmark")
    log.info(DIVIDER)

    from scripts.fetch_psycopg2 import fetch_sales as fetch_v2   # noqa: PLC0415
    from scripts.fetch_psycopg3 import fetch_sales as fetch_v3   # noqa: PLC0415
    from scripts.fetch_adbc     import fetch_sales as fetch_adbc # noqa: PLC0415

    drivers = [
        ("psycopg2 + SQLAlchemy", fetch_v2),
        ("psycopg3 + SQLAlchemy", fetch_v3),
        ("ADBC + PyArrow",        fetch_adbc),
    ]

    timings: dict[str, float] = {}
    query = "SELECT * FROM sales"

    for label, fn in drivers:
        try:
            t0 = time.perf_counter()
            fn(query)
            ms = (time.perf_counter() - t0) * 1_000
            timings[label] = ms
            log.info("  %-28s  %7.2f ms", label, ms)
        except Exception as exc:
            log.error("  %-28s  ERROR: %s", label, exc)

    if timings:
        fastest = min(timings, key=timings.get)
        log.info("")
        log.info("  ★  Fastest driver: %s (%.2f ms)", fastest, timings[fastest])
        log.info("  Note: ADBC advantage grows significantly at 100k+ rows.")


def step_notebook() -> None:
    """Step 4: Launch Jupyter notebook for interactive analysis."""
    log.info(DIVIDER)
    log.info("STEP 4 — Launching Jupyter Notebook")
    log.info(DIVIDER)

    notebook_path = PROJECT_ROOT / "notebooks" / "main.ipynb"

    if not VENV_JUPYTER.exists():
        log.warning("Jupyter not found in venv. Trying system jupyter ...")
        jupyter_cmd = shutil.which("jupyter") or "jupyter"
    else:
        jupyter_cmd = str(VENV_JUPYTER)

    log.info("Opening: %s", notebook_path)
    log.info("Press Ctrl+C to stop the notebook server.")
    try:
        subprocess.run([jupyter_cmd, "notebook", str(notebook_path)])
    except KeyboardInterrupt:
        log.info("Notebook server stopped.")
    except FileNotFoundError:
        log.error("Jupyter not found. Install it: uv pip install jupyter notebook")


# ── CLI argument parser ───────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="python main.py",
        description="PostgreSQL → Pandas Pipeline  (uv-bootstrapped)",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python main.py                  # full run (recommended first time)\n"
            "  python main.py --setup          # only create DB + seed data\n"
            "  python main.py --fetch          # only run 3 fetch approaches\n"
            "  python main.py --benchmark      # only compare driver speeds\n"
            "  python main.py --notebook       # only open Jupyter notebook\n"
            "  python main.py --no-notebook    # full run, skip Jupyter at end\n"
        ),
    )
    p.add_argument("--setup",       action="store_true", help="Only run DB setup")
    p.add_argument("--fetch",       action="store_true", help="Only run fetch scripts")
    p.add_argument("--benchmark",   action="store_true", help="Only run benchmark")
    p.add_argument("--notebook",    action="store_true", help="Only launch Jupyter")
    p.add_argument("--no-notebook", action="store_true", help="Skip Jupyter at end")
    return p.parse_args()


# ==============================================================================
# ENTRY POINT
# ==============================================================================

def main() -> None:

    # ── Phase 1: Bootstrap (only when NOT already inside the venv) ────────
    if not os.environ.get(_BOOTSTRAPPED):
        bootstrap()   # re-launches inside venv; does not return
        return        # unreachable, but explicit

    # ── Phase 2: We are inside the venv — run the actual pipeline ─────────
    args = parse_args()

    log.info(DIVIDER)
    log.info("  PostgreSQL → Pandas Pipeline")
    log.info("  Python : %s", sys.executable)
    log.info("  Workdir: %s", PROJECT_ROOT)
    log.info(DIVIDER)

    # If no specific flag is given → run everything
    run_all = not any([args.setup, args.fetch, args.benchmark, args.notebook])

    if args.notebook:
        # --notebook flag: jump straight to Jupyter
        step_notebook()
        return

    if args.setup or run_all:
        ok = step_setup()
        if not ok:
            log.error("Aborting: database setup failed.")
            sys.exit(1)

    if args.fetch or run_all:
        step_fetch()

    if args.benchmark or run_all:
        step_benchmark()

    log.info(DIVIDER)
    log.info("  ✓  Pipeline complete!")
    log.info(DIVIDER)

    # Launch Jupyter unless --no-notebook was passed
    if run_all and not args.no_notebook:
        step_notebook()


if __name__ == "__main__":
    main()
