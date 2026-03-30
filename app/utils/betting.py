"""Centralized betting math: odds conversion, EV, overlay, Kelly."""

import pandas as pd


def to_decimal_odds(odds_value) -> float:
    """Convert any odds format to decimal odds.

    Handles:
        - To-one format: 5.0 -> 6.0 (from parse_fractional_odds)
        - American positive: 350 -> 4.5
        - American negative: -200 -> 1.5
        - String values: cast to float first
        - None/NaN/invalid: returns 2.0 fallback
    """
    try:
        odds = float(odds_value) if pd.notna(odds_value) else None
    except (ValueError, TypeError):
        return 2.0

    if odds is None:
        return 2.0
    elif odds <= -100:
        return (100 / abs(odds)) + 1
    elif odds >= 100:
        return (odds / 100) + 1
    elif odds <= 0:
        return 2.0
    else:
        return odds + 1


def calculate_metrics(
    prob: float,
    decimal_odds: float,
    kelly_fraction: float = 0.25,
    max_per_race: float = 0.02,
    bankroll: float = 1000.0,
) -> dict:
    """Calculate all betting metrics for a single horse.

    Returns dict with keys: implied_prob, ev, overlay, kelly, stake.
    """
    implied_prob = 1.0 / decimal_odds if decimal_odds > 0 else 0
    ev = (prob * decimal_odds) - 1
    overlay = prob / implied_prob if implied_prob > 0 else 0

    b = decimal_odds - 1
    kelly_full = ((b * prob) - (1 - prob)) / b if b > 0 else 0
    kelly = max(0, kelly_full * kelly_fraction)
    stake = min(bankroll * kelly, bankroll * max_per_race)
    stake = max(stake, 0)

    return {
        "implied_prob": implied_prob,
        "ev": ev,
        "overlay": overlay,
        "kelly": kelly,
        "stake": stake,
    }


def qualifies_for_bet(
    ev: float,
    prob: float,
    overlay: float,
    decimal_odds: float,
    kelly: float,
    min_ev: float = 0.08,
    min_prob: float = 0.08,
    min_overlay: float = 1.20,
    max_odds: float = 15.0,
) -> bool:
    """Return True if all betting filters pass."""
    return (
        ev >= min_ev
        and prob >= min_prob
        and overlay >= min_overlay
        and decimal_odds <= max_odds + 1
        and kelly > 0
    )
