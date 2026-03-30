"""Centralized database access for the Streamlit dashboard."""

import os
import sqlite3
from contextlib import contextmanager

import pandas as pd
import streamlit as st

from app.components.sidebar import PROJECT_ROOT

_DEFAULT_DB = str(PROJECT_ROOT / "racing_data.db")


def db_path_default() -> str:
    """Return the default database path."""
    return _DEFAULT_DB


def db_exists(db_path: str = None) -> bool:
    """Return True if the database file exists."""
    return os.path.exists(db_path or _DEFAULT_DB)


@contextmanager
def get_connection(db_path: str = None):
    """Context manager that opens and closes a SQLite connection."""
    conn = sqlite3.connect(db_path or _DEFAULT_DB)
    try:
        yield conn
    finally:
        conn.close()


def query_single(sql: str, params=None, db_path: str = None):
    """Execute query, return single scalar value."""
    with get_connection(db_path) as conn:
        row = conn.execute(sql, params or []).fetchone()
        return row[0] if row else None


def query_df(sql: str, params=None, db_path: str = None) -> pd.DataFrame:
    """Execute query, return pandas DataFrame."""
    with get_connection(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params or [])


@contextmanager
def streamlit_error_boundary(operation_name: str):
    """Context manager that catches exceptions and displays them via Streamlit."""
    try:
        yield
    except Exception as e:
        st.error(f"{operation_name} failed: {e}")
        import traceback
        with st.expander("Technical details"):
            st.code(traceback.format_exc())
