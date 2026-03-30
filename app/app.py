"""
Horse Racing Quantitative Betting System - Web Dashboard

Main entry point for the Streamlit multi-page application.

Usage:
    streamlit run app/app.py --server.port 8501
"""

import sys
from pathlib import Path

import streamlit as st

# Ensure project root is on path so imports work
project_root = str(Path(__file__).resolve().parent.parent)
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from app.components.sidebar import render_sidebar

# Page configuration
st.set_page_config(
    page_title="Racing Betting System",
    page_icon="\U0001F3C7",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Render shared sidebar
render_sidebar()

# Landing page content
st.title("Horse Racing Quantitative Betting System")
st.markdown("---")

st.markdown("""
### Welcome

Use the sidebar to navigate between pages:

- **Dashboard** -- System overview, database stats, model health
- **Data Management** -- Upload XML files, run extraction, browse database
- **Model Training** -- Train or retrain the LightGBM prediction model
- **Backtesting** -- Simulate betting strategies on historical data
- **Race Predictions** -- Generate win probabilities and bet recommendations for individual races
- **Settings** -- Configure betting parameters, bankroll, and model hyperparameters
""")

col1, col2, col3 = st.columns(3)

with col1:
    st.info("**Quick Start**\n\nIf you have a trained model, go to **Race Predictions** to analyze a race.")

with col2:
    st.info("**New Data?**\n\nUpload XML files in **Data Management**, then retrain in **Model Training**.")

with col3:
    st.info("**Evaluate**\n\nCompare strategies in **Backtesting** to find the best approach.")
