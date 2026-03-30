"""Data Management - Upload XML files, run extraction, browse database."""

import os
import subprocess
import sys
from datetime import date as dt_date
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from app.components.sidebar import render_sidebar, PROJECT_ROOT
from app.utils.db import get_connection, query_df, query_single, db_exists, db_path_default

render_sidebar()

st.title("Data Management")
st.markdown("---")

UPLOAD_DIR = "data/uploads"


# --- File Upload ---
st.subheader("Upload XML Files")
st.caption(
    "Upload Equibase XML files. "
    "Past Performance files start with `SIMD`, Result Charts with `TCHM`."
)

uploaded_files = st.file_uploader(
    "Select XML files",
    type=["xml"],
    accept_multiple_files=True,
    key="xml_upload",
)

if uploaded_files:
    os.makedirs(str(PROJECT_ROOT / UPLOAD_DIR), exist_ok=True)

    file_info = []
    for f in uploaded_files:
        name = f.name
        if name.startswith("SIMD"):
            ftype = "Past Performance"
        elif name.startswith("TCHM"):
            ftype = "Result Chart"
        else:
            ftype = "Unknown"

        dest = str(PROJECT_ROOT / UPLOAD_DIR / name)
        with open(dest, "wb") as out:
            out.write(f.read())

        file_info.append({"Filename": name, "Type": ftype, "Size (KB)": f.size / 1024})

    st.success(f"Uploaded {len(uploaded_files)} file(s) to `{UPLOAD_DIR}/`")
    st.dataframe(pd.DataFrame(file_info), use_container_width=True, hide_index=True)


# --- Extraction Pipeline ---
st.markdown("---")
st.subheader("Run Extraction Pipeline")
st.caption(
    "Runs the full extraction pipeline: horses, past performance, result charts. "
    "This processes XML files in `data/uploads/` and loads data into the database."
)

col1, col2 = st.columns([1, 3])
with col1:
    run_extraction = st.button("Run Full Extraction", type="primary")

if run_extraction:
    scripts = [
        ("Extract Horses", "extract_horses.py"),
        ("Extract Past Performance", "extract_past_performance.py"),
        ("Extract Result Charts", "extract_result_charts.py"),
    ]

    for label, script in scripts:
        script_path = PROJECT_ROOT / script
        if not script_path.exists():
            st.warning(f"Script not found: `{script}`")
            continue

        with st.status(f"Running {label}...", expanded=True) as status:
            try:
                result = subprocess.run(
                    [sys.executable, str(script_path)],
                    capture_output=True,
                    text=True,
                    timeout=600,
                    cwd=str(PROJECT_ROOT),
                )
                if result.returncode == 0:
                    status.update(label=f"{label} -- Complete", state="complete")
                    if result.stdout:
                        st.code(result.stdout[-2000:], language="text")
                else:
                    status.update(label=f"{label} -- Failed", state="error")
                    st.error(result.stderr[-2000:] if result.stderr else "Unknown error")
            except subprocess.TimeoutExpired:
                status.update(label=f"{label} -- Timeout", state="error")
                st.error("Script timed out after 10 minutes.")
            except Exception as e:
                status.update(label=f"{label} -- Error", state="error")
                st.error(str(e))

    st.cache_data.clear()


# --- Database Browser ---
st.markdown("---")
st.subheader("Database Browser")

if not db_exists():
    st.warning(f"Database `{db_path_default()}` not found.")
else:
    tables_df = query_df("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = tables_df["name"].tolist() if not tables_df.empty else []

    if not tables:
        st.info("Database has no tables.")
    else:
        selected_table = st.selectbox("Table", tables, index=0)

        # Validate table name against known tables
        if selected_table not in tables:
            st.error("Invalid table selection.")
            st.stop()

        quoted_table = f'"{selected_table}"'

        # Filters for race-related tables
        filter_clause = ""
        filter_params = []

        if selected_table == "races_standardized":
            fc1, fc2, fc3 = st.columns(3)
            with fc1:
                try:
                    min_date = query_single(f"SELECT MIN(race_date) FROM {quoted_table}")
                    max_date = query_single(f"SELECT MAX(race_date) FROM {quoted_table}")
                    if min_date and max_date:
                        start_filter = st.date_input("From", value=dt_date.fromisoformat(min_date))
                        end_filter = st.date_input("To", value=dt_date.fromisoformat(max_date))
                        filter_clause = "WHERE race_date BETWEEN ? AND ?"
                        filter_params = [str(start_filter), str(end_filter)]
                except (Exception, ValueError):
                    pass

        # Query with limit
        limit = st.number_input("Max rows to display", value=100, min_value=10, max_value=5000, step=100)

        query = f"SELECT * FROM {quoted_table} {filter_clause} LIMIT ?"
        filter_params.append(int(limit))

        try:
            count = query_single(f"SELECT COUNT(*) FROM {quoted_table}")
            df = query_df(query, params=filter_params)
            st.caption(f"**{count:,}** rows in `{selected_table}`")
            st.dataframe(df, use_container_width=True, hide_index=True)

            # CSV download
            csv = df.to_csv(index=False)
            st.download_button(
                "Download as CSV",
                csv,
                file_name=f"{selected_table}.csv",
                mime="text/csv",
            )
        except Exception as e:
            st.error(f"Query error: {e}")
