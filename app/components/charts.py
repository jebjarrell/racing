"""Reusable Plotly chart builders for the Streamlit dashboard."""

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def bankroll_curve(bankroll_history: list, title: str = "Bankroll Over Time") -> go.Figure:
    """Line chart of bankroll progression."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        y=bankroll_history,
        mode="lines",
        name="Bankroll",
        line=dict(color="#2563eb", width=2),
        fill="tozeroy",
        fillcolor="rgba(37, 99, 235, 0.1)",
    ))
    fig.update_layout(
        title=title,
        xaxis_title="Bet #",
        yaxis_title="Bankroll ($)",
        template="plotly_white",
        height=400,
        yaxis=dict(tickprefix="$"),
    )
    return fig


def daily_pnl_chart(dates: list, pnl: list) -> go.Figure:
    """Bar chart of daily profit/loss."""
    colors = ["#16a34a" if v >= 0 else "#dc2626" for v in pnl]
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=dates,
        y=pnl,
        marker_color=colors,
        name="Daily P&L",
    ))
    fig.update_layout(
        title="Daily Profit / Loss",
        xaxis_title="Date",
        yaxis_title="P&L ($)",
        template="plotly_white",
        height=350,
        yaxis=dict(tickprefix="$"),
    )
    return fig


def calibration_plot(predicted_means, observed_means, bin_counts=None) -> go.Figure:
    """Reliability diagram: predicted vs actual probability."""
    predicted_means = np.asarray(predicted_means)
    observed_means = np.asarray(observed_means)

    # Filter out NaN bins
    valid = ~np.isnan(observed_means)
    pred = predicted_means[valid]
    obs = observed_means[valid]

    fig = go.Figure()

    # Perfect calibration line
    fig.add_trace(go.Scatter(
        x=[0, 1], y=[0, 1],
        mode="lines",
        line=dict(dash="dash", color="red", width=2),
        name="Perfect",
        showlegend=True,
    ))

    # Actual calibration
    fig.add_trace(go.Scatter(
        x=pred,
        y=obs,
        mode="lines+markers",
        marker=dict(size=8, color="#2563eb"),
        line=dict(color="#2563eb", width=2),
        name="Model",
    ))

    fig.update_layout(
        title="Calibration Plot",
        xaxis_title="Predicted Probability",
        yaxis_title="Observed Frequency",
        template="plotly_white",
        height=400,
        xaxis=dict(range=[0, 1]),
        yaxis=dict(range=[0, 1]),
    )
    return fig


def feature_importance_chart(importance_dict: dict, top_n: int = 20) -> go.Figure:
    """Horizontal bar chart of feature importances."""
    sorted_items = sorted(importance_dict.items(), key=lambda x: x[1], reverse=True)[:top_n]
    features = [item[0] for item in reversed(sorted_items)]
    scores = [item[1] for item in reversed(sorted_items)]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=scores,
        y=features,
        orientation="h",
        marker_color="#2563eb",
    ))
    fig.update_layout(
        title=f"Top {min(top_n, len(features))} Feature Importances",
        xaxis_title="Importance (Gain)",
        template="plotly_white",
        height=max(400, top_n * 25),
        margin=dict(l=200),
    )
    return fig


def roc_curve_chart(fpr, tpr, auc_score: float) -> go.Figure:
    """Interactive ROC curve."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=fpr, y=tpr,
        mode="lines",
        name=f"ROC (AUC = {auc_score:.4f})",
        line=dict(color="#2563eb", width=2),
    ))
    fig.add_trace(go.Scatter(
        x=[0, 1], y=[0, 1],
        mode="lines",
        name="Random",
        line=dict(dash="dash", color="red", width=1),
    ))
    fig.update_layout(
        title="ROC Curve",
        xaxis_title="False Positive Rate",
        yaxis_title="True Positive Rate",
        template="plotly_white",
        height=400,
    )
    return fig


def bet_distribution_chart(bet_amounts: list, ev_values: list) -> go.Figure:
    """Histograms of bet sizes and EV values side by side."""
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Bet Sizes", "Expected Value"))

    fig.add_trace(
        go.Histogram(x=bet_amounts, nbinsx=30, marker_color="#2563eb", name="Bet Size"),
        row=1, col=1,
    )
    fig.add_trace(
        go.Histogram(x=ev_values, nbinsx=30, marker_color="#16a34a", name="EV"),
        row=1, col=2,
    )

    fig.update_layout(
        template="plotly_white",
        height=300,
        showlegend=False,
    )
    fig.update_xaxes(title_text="Amount ($)", row=1, col=1)
    fig.update_xaxes(title_text="EV", row=1, col=2)
    return fig
