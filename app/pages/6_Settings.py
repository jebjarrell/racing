"""Settings - Edit configuration parameters."""

import sys
from pathlib import Path

import streamlit as st
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from app.components.sidebar import render_sidebar

render_sidebar()

st.title("Settings")
st.markdown("---")

CONFIG_PATH = "config/config.yaml"


def load_config() -> dict:
    """Load current configuration."""
    try:
        with open(CONFIG_PATH) as f:
            return yaml.safe_load(f)
    except Exception as e:
        st.error(f"Failed to load config: {e}")
        return {}


def save_config(config: dict):
    """Save configuration to YAML file."""
    with open(CONFIG_PATH, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


config = load_config()
if not config:
    st.stop()


# --- Betting Parameters ---
st.subheader("Betting Parameters")

betting = config.get("betting", {})

bc1, bc2 = st.columns(2)
with bc1:
    fractional_kelly = st.slider(
        "Fractional Kelly",
        0.05, 0.50,
        betting.get("fractional_kelly", 0.25),
        0.05,
        help="Fraction of full Kelly criterion to use. Lower = less variance.",
    )
    min_ev_threshold = st.slider(
        "Min EV Threshold",
        0.0, 0.30,
        betting.get("min_ev_threshold", 0.08),
        0.01,
        help="Minimum expected value required to place a bet.",
    )
    min_probability = st.slider(
        "Min Probability",
        0.0, 0.30,
        betting.get("min_probability", 0.08),
        0.01,
        help="Minimum model probability. Avoids extreme longshots.",
    )

with bc2:
    min_overlay = st.slider(
        "Min Overlay",
        1.0, 2.0,
        betting.get("min_overlay", 1.20),
        0.05,
        help="Minimum overlay ratio (model_prob / implied_prob).",
    )
    max_odds = st.slider(
        "Max Odds",
        2.0, 50.0,
        betting.get("max_odds", 15.0),
        1.0,
        help="Maximum odds to bet on. Avoids lottery tickets.",
    )
    max_per_race_pct = st.slider(
        "Max Per-Race Exposure",
        0.005, 0.10,
        betting.get("max_per_race_pct", 0.02),
        0.005,
        format="%.3f",
        help="Maximum fraction of bankroll per race.",
    )

daily_loss_limit = st.slider(
    "Daily Loss Limit",
    0.01, 0.30,
    betting.get("daily_loss_limit_pct", 0.10),
    0.01,
    help="Stop betting for the day if losses reach this % of bankroll.",
)

min_bet = st.number_input(
    "Min Bet Amount ($)",
    value=betting.get("min_bet_amount", 2.0),
    min_value=1.0,
    max_value=50.0,
    step=1.0,
)


# --- Bankroll ---
st.markdown("---")
st.subheader("Bankroll")

bankroll = config.get("bankroll", {})

bk1, bk2 = st.columns(2)
with bk1:
    initial_bankroll = st.number_input(
        "Initial Bankroll ($)",
        value=float(bankroll.get("initial", 2000)),
        min_value=100.0,
        max_value=100000.0,
        step=100.0,
    )
    reduce_threshold = st.slider(
        "Reduce Stakes at Drawdown",
        0.05, 0.50,
        bankroll.get("reduce_stakes_threshold", 0.20),
        0.05,
        help="Reduce stakes by 50% when drawdown exceeds this.",
    )

with bk2:
    pause_threshold = st.slider(
        "Pause at Drawdown",
        0.10, 0.60,
        bankroll.get("pause_threshold", 0.30),
        0.05,
        help="Stop betting entirely when drawdown exceeds this.",
    )
    max_stake_mult = st.slider(
        "Max Stake Multiplier",
        1.0, 5.0,
        bankroll.get("max_stake_multiplier", 2.0),
        0.5,
        help="Never exceed this multiple of initial stakes.",
    )


# --- Track Classifications ---
st.markdown("---")
st.subheader("Track Classifications")

tracks = config.get("tracks", {})

tk1, tk2 = st.columns(2)

all_tracks = sorted(set(
    tracks.get("high_volume", []) +
    tracks.get("regional", []) +
    tracks.get("excluded", [])
))

with tk1:
    high_volume = st.multiselect(
        "High Volume Tracks",
        options=all_tracks + ["CD", "SAR", "BEL", "GP", "SA", "DMR", "KEE", "AQU"],
        default=tracks.get("high_volume", []),
    )
with tk2:
    regional = st.multiselect(
        "Regional Tracks",
        options=all_tracks + ["TP", "CT", "PEN", "LRL", "TAM", "FG", "OP", "GG", "PRM", "IND"],
        default=tracks.get("regional", []),
    )


# --- Model Hyperparameters ---
st.markdown("---")
st.subheader("Model Hyperparameters")

model_config = config.get("model", {})
hyper = model_config.get("hyperparameters", {})

hc1, hc2, hc3 = st.columns(3)
with hc1:
    hp_n_estimators = st.number_input("n_estimators", value=hyper.get("n_estimators", 500), min_value=50, max_value=5000, step=50)
    hp_max_depth = st.number_input("max_depth", value=hyper.get("max_depth", 6), min_value=2, max_value=15)
with hc2:
    hp_lr = st.number_input("learning_rate", value=hyper.get("learning_rate", 0.05), min_value=0.001, max_value=0.5, step=0.005, format="%.3f")
    hp_subsample = st.slider("subsample", 0.1, 1.0, hyper.get("subsample", 0.8), 0.05)
with hc3:
    hp_reg_alpha = st.number_input("reg_alpha", value=hyper.get("reg_alpha", 0.1), min_value=0.0, max_value=10.0, step=0.1)
    hp_reg_lambda = st.number_input("reg_lambda", value=hyper.get("reg_lambda", 0.1), min_value=0.0, max_value=10.0, step=0.1)


# --- Save ---
st.markdown("---")

if st.button("Save Configuration", type="primary"):
    # Build updated config
    config["betting"]["fractional_kelly"] = fractional_kelly
    config["betting"]["min_ev_threshold"] = min_ev_threshold
    config["betting"]["min_probability"] = min_probability
    config["betting"]["min_overlay"] = min_overlay
    config["betting"]["max_odds"] = max_odds
    config["betting"]["max_per_race_pct"] = max_per_race_pct
    config["betting"]["daily_loss_limit_pct"] = daily_loss_limit
    config["betting"]["min_bet_amount"] = min_bet

    config["bankroll"]["initial"] = initial_bankroll
    config["bankroll"]["reduce_stakes_threshold"] = reduce_threshold
    config["bankroll"]["pause_threshold"] = pause_threshold
    config["bankroll"]["max_stake_multiplier"] = max_stake_mult

    config["tracks"]["high_volume"] = sorted(list(set(high_volume)))
    config["tracks"]["regional"] = sorted(list(set(regional)))

    config["model"]["hyperparameters"]["n_estimators"] = hp_n_estimators
    config["model"]["hyperparameters"]["max_depth"] = hp_max_depth
    config["model"]["hyperparameters"]["learning_rate"] = hp_lr
    config["model"]["hyperparameters"]["subsample"] = hp_subsample
    config["model"]["hyperparameters"]["reg_alpha"] = hp_reg_alpha
    config["model"]["hyperparameters"]["reg_lambda"] = hp_reg_lambda

    try:
        save_config(config)
        st.success("Configuration saved successfully.")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"Failed to save: {e}")
