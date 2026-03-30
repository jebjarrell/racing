"""Reusable metric display widgets for model and backtest results."""

import streamlit as st

from app.components.tooltips import METRICS, BETTING


def display_model_metrics(metrics: dict, show_features: bool = False, feature_count: int = 0):
    """Render model evaluation metrics in columns."""
    if show_features:
        cols = st.columns(5)
    else:
        cols = st.columns(4)

    cols[0].metric("ROC-AUC", f"{metrics.get('roc_auc', 0):.4f}", help=METRICS["roc_auc"])
    cols[1].metric("Brier Score", f"{metrics.get('brier_score', 0):.4f}", help=METRICS["brier_score"])
    cols[2].metric("ECE", f"{metrics.get('ece', 0):.4f}", help=METRICS["ece"])
    cols[3].metric("Log Loss", f"{metrics.get('log_loss', 0):.3f}", help=METRICS["log_loss"])

    if show_features:
        cols[4].metric("Features", feature_count)


def display_backtest_summary(results):
    """Render backtest summary metrics in 6 columns."""
    mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
    mc1.metric("ROI", f"{results.roi:+.2%}", help=BETTING["roi"])
    mc2.metric("Profit", f"${results.profit:+,.2f}")
    mc3.metric("Total Bets", f"{results.total_bets:,}")
    mc4.metric("Win Rate", f"{results.win_rate:.1%}", help=BETTING["win_rate"])
    mc5.metric("Avg Odds", f"{results.avg_odds_bet:.1f}", help=BETTING["avg_odds"])
    mc6.metric("Max Drawdown", f"{results.max_drawdown:.1%}", help=BETTING["max_drawdown"])
