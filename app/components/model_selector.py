"""Reusable model version selector widget."""

import streamlit as st


def select_model(models: list, require_features: bool = False) -> dict:
    """Render model version selectbox and return selected model info.

    Calls st.stop() if no models available or selection invalid.
    """
    if not models:
        st.warning("No trained model found. Train one in **Model Training** first.")
        st.stop()

    model_versions = [m["version"] for m in models]
    selected_version = st.selectbox("Model Version", model_versions)
    selected = next((m for m in models if m["version"] == selected_version), None)

    if selected is None:
        st.error(f"Model version '{selected_version}' not found.")
        st.stop()

    if require_features and not selected.get("feature_columns"):
        st.error("Model metadata is missing `feature_columns`. Retrain the model to fix this.")
        st.stop()

    return selected
