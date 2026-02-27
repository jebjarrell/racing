"""Shared sidebar component for all pages."""

import json
import os
from datetime import datetime
from pathlib import Path

import streamlit as st
import yaml


def get_available_models(artifacts_dir: str = "artifacts/models") -> list:
    """Scan artifacts directory for available model versions."""
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


def load_config(config_path: str = "config/config.yaml") -> dict:
    """Load YAML configuration."""
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

            from app.components.tooltips import METRICS

            col1, col2 = st.columns(2)
            col1.metric("AUC", f"{metrics.get('roc_auc', 0):.3f}", help=METRICS["roc_auc"])
            col2.metric("ECE", f"{metrics.get('ece', 0):.4f}", help=METRICS["ece"])

            brier = metrics.get("brier_score", 0)
            col1.metric("Brier", f"{brier:.4f}", help=METRICS["brier_score"])
            col2.metric("Log Loss", f"{metrics.get('log_loss', 0):.3f}", help=METRICS["log_loss"])

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
        db_path = "racing_data.db"
        if os.path.exists(db_path):
            size_mb = os.path.getsize(db_path) / (1024 * 1024)
            st.caption(f"DB: {size_mb:.1f} MB")
        else:
            st.caption("DB: not found")
