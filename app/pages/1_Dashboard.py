"""Dashboard - System overview, database stats, model health."""

import os
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from app.components.sidebar import render_sidebar, get_available_models, load_config
from app.components.tooltips import METRICS, BETTING
from app.utils.db import query_single, query_df, db_exists, db_path_default
from app.components.metrics_display import display_model_metrics

render_sidebar()

st.title("Dashboard")
st.markdown("---")


@st.cache_data(ttl=300)
def get_db_stats(db_path: str) -> dict:
    """Query database for summary statistics."""
    if not db_exists(db_path):
        return {}

    stats = {}
    errors = []

    # Race count and date range
    try:
        stats["total_races"] = query_single(
            "SELECT COUNT(*) FROM races_standardized", db_path=db_path
        )
        stats["min_date"] = query_single(
            "SELECT MIN(race_date) FROM races_standardized", db_path=db_path
        )
        stats["max_date"] = query_single(
            "SELECT MAX(race_date) FROM races_standardized", db_path=db_path
        )
    except Exception as e:
        errors.append(str(e))

    # Entry count
    try:
        stats["total_entries"] = query_single(
            "SELECT COUNT(*) FROM race_entries_standardized", db_path=db_path
        )
    except Exception as e:
        errors.append(str(e))

    # Horse count
    try:
        stats["total_horses"] = query_single(
            "SELECT COUNT(*) FROM horses_master", db_path=db_path
        )
    except Exception as e:
        errors.append(str(e))

    # Track count
    try:
        stats["total_tracks"] = query_single(
            "SELECT COUNT(DISTINCT track_code) FROM races_standardized", db_path=db_path
        )
    except Exception as e:
        errors.append(str(e))

    # DB file size
    stats["db_size_mb"] = os.path.getsize(db_path) / (1024 * 1024)

    if errors:
        stats["error"] = "; ".join(errors)

    return stats


@st.cache_data(ttl=300)
def get_recent_races(db_path: str, limit: int = 20) -> pd.DataFrame:
    """Get most recent races."""
    if not db_exists(db_path):
        return pd.DataFrame()

    try:
        return query_df("""
            SELECT
                r.race_date,
                r.track_code,
                r.race_number,
                r.course_type_code AS surface,
                r.distance_yards,
                r.race_type_code AS race_type,
                r.class_level,
                r.purse_usd,
                (SELECT COUNT(*) FROM race_entries_standardized e
                 WHERE e.race_id = r.race_id AND e.scratched = 0) AS field_size
            FROM races_standardized r
            ORDER BY r.race_date DESC, r.track_code, r.race_number
            LIMIT ?
        """, params=[limit], db_path=db_path)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300)
def get_top_performers(db_path: str, entity: str = "trainer", limit: int = 10) -> pd.DataFrame:
    """Get top trainers or jockeys by win rate."""
    if not db_exists(db_path):
        return pd.DataFrame()

    allowed = {"trainer": "trainer_id", "jockey": "jockey_id"}
    if entity not in allowed:
        return pd.DataFrame()
    id_col = allowed[entity]

    try:
        query = f"""
            SELECT
                "{id_col}" AS id,
                COUNT(*) AS starts,
                SUM(CASE WHEN official_finish_position = 1 THEN 1 ELSE 0 END) AS wins,
                ROUND(100.0 * SUM(CASE WHEN official_finish_position = 1 THEN 1 ELSE 0 END) / COUNT(*), 1) AS win_pct
            FROM race_entries_standardized
            WHERE "{id_col}" IS NOT NULL
              AND "{id_col}" != ''
              AND scratched = 0
              AND official_finish_position IS NOT NULL
            GROUP BY "{id_col}"
            HAVING starts >= 20
            ORDER BY win_pct DESC
            LIMIT ?
        """
        return query_df(query, params=[limit], db_path=db_path)
    except Exception:
        return pd.DataFrame()


# --- Database Stats ---
st.subheader("Database")

if not db_exists():
    st.error(f"Database not found at `{db_path_default()}`. Upload data in Data Management.")
else:
    stats = get_db_stats(db_path_default())

    if "error" in stats:
        st.warning(f"Some stats unavailable: {stats['error']}")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Races", f"{stats.get('total_races', 0):,}")
    c2.metric("Entries", f"{stats.get('total_entries', 0):,}")
    c3.metric("Horses", f"{stats.get('total_horses', 0):,}")
    c4.metric("Tracks", f"{stats.get('total_tracks', 0):,}")
    c5.metric("DB Size", f"{stats.get('db_size_mb', 0):.1f} MB")

    st.caption(f"Date range: **{stats.get('min_date', '?')}** to **{stats.get('max_date', '?')}**")

# --- Model Status ---
st.markdown("---")
st.subheader("Trained Models")

models = get_available_models()
if models:
    for m in models:
        metrics = m.get("metrics", {})
        with st.expander(f"**{m['version']}** -- AUC {metrics.get('roc_auc', 0):.3f} | ECE {metrics.get('ece', 0):.4f}", expanded=(m == models[0])):
            display_model_metrics(metrics)

            st.caption(f"Trained: {m.get('timestamp', 'unknown')}")
            st.caption(f"Features: {len(m.get('feature_columns', []))}")

            # Show saved plots if they exist
            model_path = Path(m["path"])
            for img_name, label in [
                ("calibration_plot.png", "Calibration Plot"),
                ("roc_curve.png", "ROC Curve"),
                ("feature_importance.png", "Feature Importance"),
            ]:
                img_path = model_path / img_name
                if img_path.exists():
                    st.image(str(img_path), caption=label, use_container_width=True)
else:
    st.info("No trained models found in `artifacts/models/`. Go to **Model Training** to train one.")

# --- Config Summary ---
st.markdown("---")
st.subheader("Configuration Summary")

config = load_config()
if config:
    cc1, cc2 = st.columns(2)
    with cc1:
        st.markdown("**Betting Parameters**")
        betting = config.get("betting", {})
        st.metric("Kelly fraction", betting.get("fractional_kelly", 0.25), help=BETTING["fractional_kelly"])
        st.metric("Min EV threshold", f"{betting.get('min_ev_threshold', 0.08):.0%}", help=BETTING["min_ev_threshold"])
        st.metric("Min probability", f"{betting.get('min_probability', 0.08):.0%}", help=BETTING["min_prob"])
        st.metric("Min overlay", f"{betting.get('min_overlay', 1.20):.2f}x", help=BETTING["min_overlay"])
        st.metric("Max odds", f"{betting.get('max_odds', 15.0):.0f}:1", help=BETTING["max_odds"])

    with cc2:
        st.markdown("**Bankroll**")
        bankroll = config.get("bankroll", {})
        st.metric("Initial", f"${bankroll.get('initial', 2000):,.0f}")
        st.metric("Reduce stakes at", f"-{bankroll.get('reduce_stakes_threshold', 0.20):.0%} drawdown", help="Bankroll drawdown threshold that triggers a 50% reduction in bet sizes to protect capital.")
        st.metric("Pause betting at", f"-{bankroll.get('pause_threshold', 0.30):.0%} drawdown", help="Bankroll drawdown threshold that pauses all betting until the model is re-evaluated.")

# --- Recent Races ---
st.markdown("---")
st.subheader("Recent Races")

recent = get_recent_races(db_path_default())
if not recent.empty:
    st.dataframe(recent, use_container_width=True, hide_index=True)
else:
    st.info("No race data available.")

# --- Top Performers ---
st.markdown("---")
st.subheader("Top Performers")

tab_trainer, tab_jockey = st.tabs(["Trainers", "Jockeys"])

with tab_trainer:
    trainers = get_top_performers(db_path_default(), "trainer")
    if not trainers.empty:
        st.dataframe(trainers, use_container_width=True, hide_index=True)
    else:
        st.info("No trainer data available.")

with tab_jockey:
    jockeys = get_top_performers(db_path_default(), "jockey")
    if not jockeys.empty:
        st.dataframe(jockeys, use_container_width=True, hide_index=True)
    else:
        st.info("No jockey data available.")
