"""Backtesting - Simulate betting strategies on historical data."""

import pickle
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from app.components.sidebar import render_sidebar, get_available_models, load_config
from app.components.charts import bankroll_curve, daily_pnl_chart, bet_distribution_chart

render_sidebar()

st.title("Backtesting")
st.markdown("---")

config = load_config()
models = get_available_models()

if not models:
    st.warning("No trained models found. Train a model first in **Model Training**.")
    st.stop()


# --- Configuration Sidebar ---
with st.sidebar:
    st.subheader("Backtest Config")

    # Model selection
    model_versions = [m["version"] for m in models]
    selected_version = st.selectbox("Model Version", model_versions)
    selected_model_info = next(m for m in models if m["version"] == selected_version)

    # Strategy selection
    strategy_name = st.selectbox(
        "Strategy",
        ["Flat Bet", "Kelly Criterion", "Value Betting", "Top Pick", "Momentum", "Morning Favorite"],
    )

    # Strategy-specific parameters
    st.markdown("**Strategy Parameters**")
    if strategy_name == "Flat Bet":
        bet_amount = st.number_input("Bet amount ($)", value=2.0, min_value=1.0, max_value=100.0, step=1.0)
        min_prob = st.slider("Min probability", 0.0, 0.50, 0.0, 0.01)
        max_odds = st.slider("Max odds", 2.0, 50.0, 50.0, 1.0)
    elif strategy_name == "Kelly Criterion":
        kelly_fraction = st.slider("Kelly fraction", 0.05, 0.50, 0.25, 0.05)
        min_edge = st.slider("Min edge", 0.0, 0.30, 0.05, 0.01)
        max_bet_frac = st.slider("Max bet % of bankroll", 0.01, 0.20, 0.10, 0.01)
    elif strategy_name == "Value Betting":
        vb_bet_fraction = st.slider("Bet fraction", 0.005, 0.10, 0.02, 0.005)
        vb_min_edge = st.slider("Min edge", 0.0, 0.30, 0.10, 0.01)
        vb_min_prob = st.slider("Min probability", 0.0, 0.30, 0.05, 0.01)
        vb_max_prob = st.slider("Max probability", 0.20, 1.0, 0.50, 0.05)
    elif strategy_name == "Top Pick":
        tp_bet_amount = st.number_input("Bet amount ($)", value=2.0, min_value=1.0, max_value=100.0, step=1.0, key="tp_bet")
        tp_min_prob = st.slider("Min probability", 0.0, 0.50, 0.15, 0.01, key="tp_prob")
    elif strategy_name == "Momentum":
        mom_base = st.slider("Base fraction", 0.005, 0.10, 0.02, 0.005)
        mom_mult = st.slider("Momentum multiplier", 1.0, 5.0, 2.0, 0.5)
    elif strategy_name == "Morning Favorite":
        mf_min_edge = st.slider("Min edge", 0.0, 0.30, 0.10, 0.01, key="mf_edge")
        mf_bet_frac = st.slider("Bet fraction", 0.005, 0.10, 0.03, 0.005, key="mf_frac")

    # Date range
    st.markdown("**Date Range**")
    test_config = config.get("model", {}).get("splits", {}).get("test", {})
    default_start = date.fromisoformat(test_config.get("start", "2023-10-01"))
    default_end = date.fromisoformat(test_config.get("end", "2023-12-31"))

    bt_start = st.date_input("Start date", value=default_start, key="bt_start")
    bt_end = st.date_input("End date", value=default_end, key="bt_end")

    # Bankroll
    initial_bankroll = st.number_input(
        "Initial bankroll ($)", value=1000.0, min_value=100.0, max_value=100000.0, step=100.0
    )


# --- Build Strategy ---
def build_strategy():
    from backtesting import (
        FlatBetStrategy, KellyCriterionStrategy, ValueBettingStrategy,
        TopPickStrategy, MomentumStrategy, MorningFavoriteStrategy,
    )

    if strategy_name == "Flat Bet":
        return FlatBetStrategy(bet_amount=bet_amount, min_prob=min_prob, max_odds=max_odds)
    elif strategy_name == "Kelly Criterion":
        return KellyCriterionStrategy(fraction=kelly_fraction, min_edge=min_edge, max_bet_fraction=max_bet_frac)
    elif strategy_name == "Value Betting":
        return ValueBettingStrategy(bet_fraction=vb_bet_fraction, min_edge=vb_min_edge, min_prob=vb_min_prob, max_prob=vb_max_prob)
    elif strategy_name == "Top Pick":
        return TopPickStrategy(bet_amount=tp_bet_amount, min_prob=tp_min_prob)
    elif strategy_name == "Momentum":
        return MomentumStrategy(base_fraction=mom_base, momentum_multiplier=mom_mult)
    elif strategy_name == "Morning Favorite":
        return MorningFavoriteStrategy(min_edge=mf_min_edge, bet_fraction=mf_bet_frac)


# --- Run Backtest ---
col_run, col_compare = st.columns(2)

run_single = col_run.button("Run Backtest", type="primary")
run_compare = col_compare.button("Compare All Strategies")


def load_model_and_backtester():
    """Load model, calibrator, and create backtester."""
    from models.lightgbm_model import RacingLightGBM
    from backtesting import Backtester

    model_path = Path(selected_model_info["path"])
    model = RacingLightGBM.load(str(model_path / "model.pkl"))

    with open(model_path / "calibrator.pkl", "rb") as f:
        calibrator = pickle.load(f)

    feature_columns = selected_model_info.get("feature_columns", [])

    backtester = Backtester(
        model=model,
        calibrator=calibrator,
        feature_columns=feature_columns,
        db_path="racing_data.db",
    )
    return backtester


def display_results(results):
    """Display backtest results with charts."""
    # Summary metrics
    mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
    mc1.metric("ROI", f"{results.roi:+.2%}")
    mc2.metric("Profit", f"${results.profit:+,.2f}")
    mc3.metric("Total Bets", f"{results.total_bets:,}")
    mc4.metric("Win Rate", f"{results.win_rate:.1%}")
    mc5.metric("Avg Odds", f"{results.avg_odds_bet:.1f}")
    mc6.metric("Max Drawdown", f"{results.max_drawdown:.1%}")

    # Bankroll curve
    if results.bankroll_history:
        st.plotly_chart(
            bankroll_curve(results.bankroll_history, f"Bankroll: {results.strategy_name}"),
            use_container_width=True,
        )

    # Daily P&L
    if results.bets:
        daily = defaultdict(float)
        for bet in results.bets:
            daily[str(bet.race_date)] += bet.profit
        dates = sorted(daily.keys())
        pnl_vals = [daily[d] for d in dates]
        st.plotly_chart(daily_pnl_chart(dates, pnl_vals), use_container_width=True)

        # Bet distribution
        amounts = [b.bet_amount for b in results.bets]
        evs = [(b.model_prob * b.odds) - 1 for b in results.bets]
        st.plotly_chart(bet_distribution_chart(amounts, evs), use_container_width=True)

    # Bet log
    if results.bets:
        st.subheader("Bet Log")
        bet_data = []
        for b in results.bets:
            bet_data.append({
                "Date": str(b.race_date),
                "Race": b.race_id,
                "Horse": b.horse_name,
                "Prob": f"{b.model_prob:.1%}",
                "Odds": f"{b.odds:.1f}",
                "EV": f"{(b.model_prob * b.odds - 1):+.1%}",
                "Stake": f"${b.bet_amount:.2f}",
                "Won": "Y" if b.won else "",
                "P&L": f"${b.profit:+.2f}",
            })
        df = pd.DataFrame(bet_data)
        st.dataframe(df, use_container_width=True, hide_index=True)

        # CSV download
        csv = df.to_csv(index=False)
        st.download_button("Download Bet Log CSV", csv, "backtest_bets.csv", "text/csv")


if run_single:
    strategy = build_strategy()
    with st.spinner(f"Running backtest: {strategy.name}..."):
        try:
            backtester = load_model_and_backtester()
            results = backtester.run(
                strategy=strategy,
                start_date=bt_start,
                end_date=bt_end,
                initial_bankroll=initial_bankroll,
            )
            st.success(f"Backtest complete: **{strategy.name}**")
            display_results(results)
        except Exception as e:
            st.error(f"Backtest failed: {e}")
            import traceback
            st.code(traceback.format_exc())

elif run_compare:
    from backtesting import (
        FlatBetStrategy, KellyCriterionStrategy, ValueBettingStrategy,
        TopPickStrategy,
    )

    strategies = [
        FlatBetStrategy(bet_amount=2.0),
        FlatBetStrategy(bet_amount=2.0, min_prob=0.15),
        KellyCriterionStrategy(fraction=0.25, min_edge=0.05),
        KellyCriterionStrategy(fraction=0.10, min_edge=0.10),
        ValueBettingStrategy(bet_fraction=0.02, min_edge=0.10),
        TopPickStrategy(bet_amount=2.0, min_prob=0.15),
    ]

    with st.spinner("Comparing all strategies..."):
        try:
            backtester = load_model_and_backtester()
            comparison = backtester.compare_strategies(
                strategies=strategies,
                start_date=bt_start,
                end_date=bt_end,
                initial_bankroll=initial_bankroll,
            )

            st.success("Strategy comparison complete")
            st.dataframe(comparison, use_container_width=True, hide_index=True)
        except Exception as e:
            st.error(f"Comparison failed: {e}")
            import traceback
            st.code(traceback.format_exc())
