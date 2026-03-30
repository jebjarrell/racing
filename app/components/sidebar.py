"""Shared sidebar component for all pages."""

import json
import os
from datetime import datetime
from pathlib import Path

import streamlit as st
import yaml

from app.components.metrics_display import display_model_metrics

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def get_available_models(artifacts_dir: str = None) -> list:
    """Scan artifacts directory for available model versions."""
    if artifacts_dir is None:
        artifacts_dir = str(PROJECT_ROOT / "artifacts" / "models")
    models = []
    artifacts_path = Path(artifacts_dir)
    if not artifacts_path.exists():
        return models

    for version_dir in sorted(artifacts_path.iterdir(), reverse=True):
        if version_dir.is_dir() and (version_dir / "metadata.json").exists() and (version_dir / "model.pkl").exists():
            try:
                with open(version_dir / "metadata.json") as f:
                    metadata = json.load(f)
                models.append({
                    "version": metadata.get("version", version_dir.name),
                    "path": str(version_dir),
                    "timestamp": metadata.get("timestamp", ""),
                    "metrics": metadata.get("metrics", {}),
                    "feature_columns": metadata.get("feature_columns", []),
                })
            except (json.JSONDecodeError, KeyError):
                continue
    return models


def load_config(config_path: str = None) -> dict:
    """Load YAML configuration."""
    if config_path is None:
        config_path = str(PROJECT_ROOT / "config" / "config.yaml")
    try:
        with open(config_path) as f:
            return yaml.safe_load(f)
    except (FileNotFoundError, yaml.YAMLError):
        return {}


def render_sidebar():
    """Render the shared sidebar on every page."""
    with st.sidebar:
        st.title("Racing System")
        st.divider()

        # Model status
        models = get_available_models()
        if models:
            current = models[0]
            metrics = current.get("metrics", {})

            st.subheader("Model Status")
            st.caption(f"Version: **{current['version']}**")
            if current["timestamp"]:
                try:
                    dt = datetime.fromisoformat(current["timestamp"])
                    st.caption(f"Trained: {dt.strftime('%Y-%m-%d %H:%M')}")
                except ValueError:
                    pass

            display_model_metrics(metrics)

            # Alert: calibration drift
            ece = metrics.get("ece", 0)
            if ece > 0.03:
                st.warning(f"Calibration drift: ECE {ece:.4f} > 0.03")
        else:
            st.subheader("Model Status")
            st.info("No trained model found. Go to Model Training.")

        st.divider()

        # Config summary
        config = load_config()
        if config:
            st.subheader("Config")
            betting = config.get("betting", {})
            bankroll = config.get("bankroll", {})

            st.caption(f"Bankroll: **${bankroll.get('initial', 0):,.0f}**")
            st.caption(f"Kelly: **{betting.get('fractional_kelly', 0.25):.0%}**")
            st.caption(f"Min EV: **{betting.get('min_ev_threshold', 0.08):.0%}**")
            st.caption(f"Max Odds: **{betting.get('max_odds', 15)}:1**")

        st.divider()

        # DB status
        db_path = str(PROJECT_ROOT / "racing_data.db")
        if os.path.exists(db_path):
            size_mb = os.path.getsize(db_path) / (1024 * 1024)
            st.caption(f"DB: {size_mb:.1f} MB")
        else:
            st.caption("DB: not found")
