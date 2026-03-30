"""Race Predictions - Generate win probabilities and bet recommendations."""

import json
import os
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from app.components.sidebar import render_sidebar, get_available_models, load_config, PROJECT_ROOT
from app.components.charts import feature_importance_chart
from app.components.tooltips import BETTING
from app.components.model_selector import select_model
from app.utils.db import get_connection, db_exists, db_path_default, streamlit_error_boundary
from app.utils.betting import to_decimal_odds, calculate_metrics, qualifies_for_bet
from app.utils.features import prepare_feature_matrix

render_sidebar()

st.title("Race Predictions")
st.markdown("---")

config = load_config()
betting_config = config.get("betting", {})
bankroll_config = config.get("bankroll", {})


# --- Validate Prerequisites ---
models = get_available_models()

if not db_exists():
    st.warning(f"Database `{db_path_default()}` not found.")
    st.stop()


# --- Model Selection ---
selected_model_info = select_model(models, require_features=True)


@st.cache_resource
def load_model(model_path: str, mtime: float = 0):
    """Load model and calibrator. mtime busts cache when model file changes."""
    from models.lightgbm_model import RacingLightGBM
    from models.calibration import FieldSizeCalibrator

    model = RacingLightGBM.load(os.path.join(model_path, "model.pkl"))
    calibrator = FieldSizeCalibrator.load(os.path.join(model_path, "calibrator.pkl"))
    return model, calibrator


# --- Race Selection ---
st.subheader("Select Race")

with get_connection() as conn:
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

    if tracks_df.empty:
        st.info("No tracks found for this date.")
        st.stop()

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
    with streamlit_error_boundary("Prediction"):
        from features.feature_engine import FeatureEngine

        model_pkl = os.path.join(selected_model_info["path"], "model.pkl")
        model_mtime = os.path.getmtime(model_pkl) if os.path.exists(model_pkl) else 0
        model, calibrator = load_model(selected_model_info["path"], mtime=model_mtime)
        feature_columns = selected_model_info.get("feature_columns", [])

        # Calculate features
        with st.spinner("Calculating features..."):
            engine = FeatureEngine(db_path=db_path_default())
            race_date = date.fromisoformat(selected_date)
            features_list = engine.calculate_all_features(selected_race_id, race_date)
            engine.close()

        if not features_list:
            st.warning("No features could be calculated for this race.")
            st.stop()

        features_df = pd.DataFrame(features_list)

        # Get horse names
        with get_connection() as conn:
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
        X = prepare_feature_matrix(features_df, feature_columns)

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

        # Warn about entries with missing predictions
        nan_count = result["model_prob"].isna().sum()
        if nan_count > 0:
            st.warning(f"{nan_count} entries excluded (missing features or merge mismatch)")
            result = result.dropna(subset=["model_prob"])

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
            raw_actual = r.get("actual_odds")
            raw_ml = r.get("morning_line_odds") or r.get("morning_line_odds_entry")
            try:
                actual_val = float(raw_actual) if pd.notna(raw_actual) else None
            except (ValueError, TypeError):
                actual_val = None
            odds_source = actual_val if actual_val and actual_val > 0 else raw_ml
            decimal_odds = to_decimal_odds(odds_source)

            prob = r["model_prob"]
            bet = calculate_metrics(prob, decimal_odds, kelly_frac, max_per_race, bankroll)
            qualifies = qualifies_for_bet(
                bet["ev"], prob, bet["overlay"], decimal_odds, bet["kelly"],
                min_ev=min_ev, min_prob=min_prob, min_overlay=min_overlay, max_odds=max_odds,
            )

            finish = r.get("official_finish_position")

            rows.append({
                "PP": r.get("post_position") or r.get("post_position_entry", ""),
                "Horse": r.get("horse_name", "Unknown"),
                "Prob": prob,
                "ML Odds": f"{raw_ml}" if pd.notna(raw_ml) else "-",
                "Actual Odds": f"{actual_val}" if actual_val is not None else "-",
                "Implied": f"{bet['implied_prob']:.1%}" if bet["implied_prob"] > 0 else "-",
                "EV": bet["ev"],
                "Overlay": bet["overlay"],
                "Kelly %": bet["kelly"],
                "Stake": bet["stake"],
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

        # Speed figure comparison chart
        speed_cols = [c for c in ['best_speed_last', 'best_speed_90_days', 'avg_speed_figure'] if c in result.columns]
        if speed_cols:
            speed_col = speed_cols[0]
            chart_data = result[['horse_name', speed_col]].dropna(subset=[speed_col])
            if not chart_data.empty:
                chart_data = chart_data.sort_values(speed_col, ascending=True)
                st.subheader("Speed Figure Comparison")
                fig = go.Figure(go.Bar(
                    x=chart_data[speed_col],
                    y=chart_data['horse_name'],
                    orientation='h',
                    marker_color='#2563eb',
                ))
                fig.update_layout(
                    xaxis_title="Speed Figure",
                    template="plotly_white",
                    height=max(300, len(chart_data) * 30),
                )
                st.plotly_chart(fig, use_container_width=True)

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
