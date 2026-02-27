"""Race Predictions - Generate win probabilities and bet recommendations."""

import json
import os
import sqlite3
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from app.components.sidebar import render_sidebar, get_available_models, load_config
from app.components.charts import feature_importance_chart
from app.components.tooltips import BETTING

render_sidebar()

st.title("Race Predictions")
st.markdown("---")

DB_PATH = "racing_data.db"
config = load_config()
betting_config = config.get("betting", {})
bankroll_config = config.get("bankroll", {})


# --- Validate Prerequisites ---
models = get_available_models()
if not models:
    st.warning("No trained model found. Train one in **Model Training** first.")
    st.stop()

if not os.path.exists(DB_PATH):
    st.warning(f"Database `{DB_PATH}` not found.")
    st.stop()


# --- Model Selection ---
model_versions = [m["version"] for m in models]
selected_version = st.selectbox("Model Version", model_versions)
selected_model_info = next((m for m in models if m["version"] == selected_version), None)
if selected_model_info is None:
    st.error(f"Model version '{selected_version}' not found.")
    st.stop()


@st.cache_resource
def load_model(model_path: str):
    """Load model and calibrator."""
    from models.lightgbm_model import RacingLightGBM
    from models.calibration import FieldSizeCalibrator

    model = RacingLightGBM.load(os.path.join(model_path, "model.pkl"))
    calibrator = FieldSizeCalibrator.load(os.path.join(model_path, "calibrator.pkl"))
    return model, calibrator


# --- Race Selection ---
st.subheader("Select Race")

with sqlite3.connect(DB_PATH) as conn:
    # Get available dates
    dates_df = pd.read_sql_query(
        "SELECT DISTINCT race_date FROM races_standardized ORDER BY race_date DESC LIMIT 365",
        conn,
    )
    available_dates = dates_df["race_date"].tolist()

    if not available_dates:
        st.info("No races in database.")
        st.stop()

    col1, col2, col3 = st.columns(3)

    with col1:
        selected_date = st.selectbox("Race Date", available_dates)

    # Get tracks for selected date
    tracks_df = pd.read_sql_query(
        "SELECT DISTINCT track_code FROM races_standardized WHERE race_date = ? ORDER BY track_code",
        conn,
        params=[selected_date],
    )

    with col2:
        selected_track = st.selectbox("Track", tracks_df["track_code"].tolist())

    # Get race numbers for selected date + track
    races_df = pd.read_sql_query(
        "SELECT race_number, race_id, course_type_code, distance_yards, race_type_code, class_level, purse_usd "
        "FROM races_standardized WHERE race_date = ? AND track_code = ? ORDER BY race_number",
        conn,
        params=[selected_date, selected_track],
    )

    with col3:
        race_options = {f"Race {r['race_number']}": r["race_id"] for _, r in races_df.iterrows()}
        if not race_options:
            st.info("No races found for this date/track.")
            st.stop()

        selected_label = st.selectbox("Race", list(race_options.keys()))
        selected_race_id = race_options[selected_label]

    # Show race context
    race_matches = races_df[races_df["race_id"] == selected_race_id]
    if race_matches.empty:
        st.error("Selected race not found in data.")
        st.stop()
    race_row = race_matches.iloc[0]
    ctx1, ctx2, ctx3, ctx4, ctx5 = st.columns(5)
    ctx1.metric("Surface", race_row.get("course_type_code", "?"))
    ctx2.metric("Distance (yds)", f"{race_row.get('distance_yards', 0):,}")
    ctx3.metric("Race Type", race_row.get("race_type_code", "?"))
    ctx4.metric("Class", race_row.get("class_level", "?"))
    ctx5.metric("Purse", f"${race_row.get('purse_usd', 0):,.0f}")

st.markdown("---")


# --- Generate Predictions ---
if st.button("Generate Predictions", type="primary"):
    try:
        from features.feature_engine import FeatureEngine

        model, calibrator = load_model(selected_model_info["path"])
        feature_columns = selected_model_info.get("feature_columns", [])

        # Calculate features
        with st.spinner("Calculating features..."):
            engine = FeatureEngine(db_path=DB_PATH)
            race_date = date.fromisoformat(selected_date)
            features_list = engine.calculate_all_features(selected_race_id, race_date)
            engine.close()

        if not features_list:
            st.warning("No features could be calculated for this race.")
            st.stop()

        features_df = pd.DataFrame(features_list)

        # Get horse names
        with sqlite3.connect(DB_PATH) as conn:
            entries_df = pd.read_sql_query(
                """
                SELECT e.entry_id, e.registration_number, e.post_position, e.program_number,
                       e.morning_line_odds, e.actual_odds, e.official_finish_position,
                       h.horse_name
                FROM race_entries_standardized e
                LEFT JOIN horses_master h ON e.registration_number = h.registration_number
                WHERE e.race_id = ? AND e.scratched = 0
                ORDER BY e.post_position
                """,
                conn,
                params=[selected_race_id],
            )

        # Prepare model features
        available_cols = [c for c in feature_columns if c in features_df.columns]
        X = features_df[available_cols].copy()
        for c in feature_columns:
            if c not in X.columns:
                X[c] = 0
        X = X[feature_columns].fillna(0)

        # Predict
        with st.spinner("Running model prediction..."):
            raw_probs = model.predict_raw(X)
            field_sizes = np.full(len(X), len(features_df))
            calibrated_probs = calibrator.calibrate(raw_probs, field_sizes)

            # Softmax normalize within race
            from models.lightgbm_model import softmax_by_race
            race_ids = np.array([selected_race_id] * len(X))
            normalized_probs = softmax_by_race(calibrated_probs, race_ids)

        # Merge predictions with entry info
        features_df["model_prob"] = normalized_probs
        features_df["raw_prob"] = raw_probs

        result = features_df.merge(
            entries_df[["entry_id", "horse_name", "program_number", "post_position",
                        "morning_line_odds", "actual_odds", "official_finish_position"]],
            on="entry_id",
            how="left",
            suffixes=("", "_entry"),
        )

        # Calculate betting metrics
        bankroll = bankroll_config.get("initial", 2000.0)
        min_ev = betting_config.get("min_ev_threshold", 0.08)
        min_prob = betting_config.get("min_probability", 0.08)
        min_overlay = betting_config.get("min_overlay", 1.20)
        max_odds = betting_config.get("max_odds", 15.0)
        kelly_frac = betting_config.get("fractional_kelly", 0.25)
        max_per_race = betting_config.get("max_per_race_pct", 0.02)

        rows = []
        for _, r in result.iterrows():
            ml_odds = r.get("morning_line_odds") or r.get("morning_line_odds_entry")
            actual = r.get("actual_odds")
            odds_to_use = actual if pd.notna(actual) and actual > 0 else ml_odds

            if pd.isna(odds_to_use) or odds_to_use <= 0:
                decimal_odds = 2.0
            else:
                # Convert to decimal if needed
                if odds_to_use >= 100:
                    decimal_odds = (odds_to_use / 100) + 1
                elif odds_to_use <= -100:
                    decimal_odds = (100 / abs(odds_to_use)) + 1
                else:
                    decimal_odds = odds_to_use + 1 if odds_to_use > 0 else odds_to_use

            prob = r["model_prob"]
            implied_prob = 1.0 / decimal_odds if decimal_odds > 0 else 0
            ev = (prob * decimal_odds) - 1
            overlay = prob / implied_prob if implied_prob > 0 else 0

            # Kelly
            b = decimal_odds - 1
            kelly_full = ((b * prob) - (1 - prob)) / b if b > 0 else 0
            kelly = max(0, kelly_full * kelly_frac)
            stake = min(bankroll * kelly, bankroll * max_per_race)
            stake = max(stake, 0)

            # Qualifying?
            qualifies = (
                ev >= min_ev
                and prob >= min_prob
                and overlay >= min_overlay
                and decimal_odds <= max_odds + 1
                and kelly > 0
            )

            finish = r.get("official_finish_position")

            rows.append({
                "PP": r.get("post_position") or r.get("post_position_entry", ""),
                "Horse": r.get("horse_name", "Unknown"),
                "Prob": prob,
                "ML Odds": f"{ml_odds}" if pd.notna(ml_odds) else "-",
                "Actual Odds": f"{actual}" if pd.notna(actual) else "-",
                "Implied": f"{implied_prob:.1%}" if implied_prob > 0 else "-",
                "EV": ev,
                "Overlay": overlay,
                "Kelly %": kelly,
                "Stake": stake,
                "Bet?": "YES" if qualifies else "",
                "Finish": int(finish) if pd.notna(finish) else "-",
            })

        pred_df = pd.DataFrame(rows).sort_values("Prob", ascending=False)

        # Display
        st.subheader("Predictions")

        # Column legend
        with st.expander("Column definitions", expanded=False):
            legend_cols = st.columns(3)
            legend_cols[0].markdown(f"**Prob** -- Model's predicted win probability")
            legend_cols[0].markdown(f"**EV** -- {BETTING['ev']}")
            legend_cols[1].markdown(f"**Overlay** -- {BETTING['overlay']}")
            legend_cols[1].markdown(f"**Implied** -- {BETTING['implied_prob']}")
            legend_cols[2].markdown(f"**Kelly %** -- {BETTING['kelly_pct']}")
            legend_cols[2].markdown(f"**Bet?** -- YES if all filters pass (min EV, min prob, min overlay, max odds)")

        # Format for display
        display_df = pred_df.copy()
        display_df["Prob"] = display_df["Prob"].apply(lambda x: f"{x:.1%}")
        display_df["EV"] = display_df["EV"].apply(lambda x: f"{x:+.1%}")
        display_df["Overlay"] = display_df["Overlay"].apply(lambda x: f"{x:.2f}x")
        display_df["Kelly %"] = display_df["Kelly %"].apply(lambda x: f"{x:.2%}")
        display_df["Stake"] = display_df["Stake"].apply(lambda x: f"${x:.2f}" if x > 0 else "-")

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Qualifying bets summary
        qualifying = pred_df[pred_df["Bet?"] == "YES"]
        if not qualifying.empty:
            st.success(f"**{len(qualifying)} qualifying bet(s) found**")
            for _, q in qualifying.iterrows():
                st.markdown(
                    f"- **{q['Horse']}** (PP {q['PP']}) -- "
                    f"Prob {q['Prob']:.1%}, EV {q['EV']:+.1%}, "
                    f"Stake **${q['Stake']:.2f}**"
                )
        else:
            st.info("No qualifying bets for this race (no entries pass all filters).")

        # Feature details
        st.markdown("---")
        st.subheader("Feature Details")

        for _, r in result.sort_values("model_prob", ascending=False).iterrows():
            horse_name = r.get("horse_name", "Unknown")
            prob = r["model_prob"]
            with st.expander(f"{horse_name} -- {prob:.1%}"):
                feature_data = {
                    k: r[k]
                    for k in feature_columns
                    if k in r.index and pd.notna(r[k])
                }
                st.json(feature_data)

        # CSV download
        csv = display_df.to_csv(index=False)
        st.download_button("Download Predictions CSV", csv, "predictions.csv", "text/csv")

    except Exception as e:
        st.error(f"Prediction failed: {e}")
        import traceback
        st.code(traceback.format_exc())
