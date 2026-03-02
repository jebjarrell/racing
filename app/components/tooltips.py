"""Tooltip definitions for all technical terms in the UI."""

# --- Model Evaluation Metrics ---
METRICS = {
    "roc_auc": (
        "ROC-AUC (Area Under the Receiver Operating Characteristic Curve) measures "
        "how well the model separates winners from non-winners. Ranges from 0.5 "
        "(random guessing) to 1.0 (perfect). Above 0.70 is considered good for "
        "horse racing."
    ),
    "brier_score": (
        "Brier Score measures the accuracy of predicted probabilities. Ranges from "
        "0 (perfect) to 1 (worst). Lower is better. A model predicting 30% for a "
        "horse that wins gets a lower Brier score than one predicting 10%."
    ),
    "ece": (
        "ECE (Expected Calibration Error) measures whether predicted probabilities "
        "match actual win rates. An ECE of 0.02 means predictions are off by ~2% on "
        "average. Below 0.03 is well-calibrated; above 0.05 suggests the model's "
        "probabilities are unreliable."
    ),
    "log_loss": (
        "Log Loss penalizes confident wrong predictions heavily. A model that says "
        "90% for a loser is punished more than one that says 60%. Lower is better. "
        "Typical range for racing models: 0.3-0.5."
    ),
    "mce": (
        "MCE (Maximum Calibration Error) is the worst-case calibration error across "
        "all probability bins. High MCE means the model is badly miscalibrated in at "
        "least one range."
    ),
}

# --- Training Hyperparameters ---
HYPERPARAMS = {
    "n_estimators": (
        "Number of boosting rounds (decision trees) to train. More trees can improve "
        "accuracy but increase training time and risk overfitting. Typical range: "
        "200-2000. Early stopping will halt training if the validation score stops "
        "improving."
    ),
    "max_depth": (
        "Maximum depth of each decision tree. Deeper trees capture more complex "
        "patterns but are more prone to overfitting. Typical range: 4-8. Start low "
        "and increase only if underfitting."
    ),
    "learning_rate": (
        "How much each tree contributes to the final prediction. Lower values "
        "(0.01-0.05) require more trees but often produce better results. Higher "
        "values (0.1+) train faster but may overfit."
    ),
    "subsample": (
        "Fraction of training data used for each tree. Values below 1.0 add "
        "randomness that helps prevent overfitting. 0.8 means each tree sees 80% "
        "of the data. Also controls feature_fraction and bagging_fraction."
    ),
    "reg_alpha": (
        "L1 regularization (Lasso). Encourages the model to ignore weak features "
        "by pushing their weights toward zero. Higher values = stronger "
        "regularization = simpler model. Typical range: 0-1.0."
    ),
    "reg_lambda": (
        "L2 regularization (Ridge). Prevents any single feature from dominating "
        "predictions by penalizing large weights. Higher values = stronger "
        "regularization = smoother predictions. Typical range: 0-1.0."
    ),
}

# --- Data Splits ---
SPLITS = {
    "train": (
        "Training set: the model learns patterns from this data. Should be the "
        "largest split. Uses earlier dates to prevent data leakage."
    ),
    "validation": (
        "Validation set: used during training to detect overfitting and tune "
        "hyperparameters. The model sees this data but doesn't learn from it "
        "directly. Must be after the training period."
    ),
    "test": (
        "Test set: held-out data the model never sees until final evaluation. "
        "Provides an unbiased estimate of real-world performance. Must be the "
        "most recent dates."
    ),
}

# --- Betting / Strategy Terms ---
BETTING = {
    "roi": (
        "Return on Investment. Total profit divided by total amount wagered. "
        "+5% ROI means you earned $5 for every $100 bet. Positive ROI means "
        "the strategy is profitable."
    ),
    "win_rate": (
        "Percentage of bets that won. In horse racing, a 15-20% win rate can "
        "still be highly profitable if the winning odds are high enough."
    ),
    "max_drawdown": (
        "Largest peak-to-trough decline in bankroll during the backtest. A 20% "
        "max drawdown means the bankroll dropped 20% from its highest point "
        "before recovering. Lower is better."
    ),
    "avg_odds": (
        "Average odds of horses bet on. Higher average odds mean riskier bets "
        "with bigger potential payouts but lower win rates."
    ),
    "kelly_fraction": (
        "Fraction of the full Kelly Criterion to use. Full Kelly (1.0) maximizes "
        "long-term growth but has extreme variance. Quarter Kelly (0.25) is common "
        "in practice -- it sacrifices ~25% of growth rate but reduces variance by ~75%."
    ),
    "min_edge": (
        "Minimum required edge before placing a bet. Edge = (model probability x "
        "odds) - 1. A 5% min edge means the model must estimate at least 5% "
        "expected profit on a bet."
    ),
    "max_bet_fraction": (
        "Maximum percentage of bankroll to risk on a single bet. Caps exposure "
        "to prevent catastrophic losses from any single race."
    ),
    "min_prob": (
        "Minimum model probability required to consider a bet. Filters out extreme "
        "longshots where the model may be unreliable."
    ),
    "max_odds": (
        "Maximum odds allowed for a bet. Filters out very high-odds longshots "
        "that have high variance and where market odds may be unreliable."
    ),
    "bet_fraction": (
        "Fixed percentage of bankroll to wager on each qualifying bet. Simpler "
        "than Kelly sizing but doesn't adapt to edge size."
    ),
    "momentum_multiplier": (
        "Multiplier applied to bet size after a winning streak. Increases stakes "
        "during hot streaks and reduces them during cold streaks."
    ),
    "ev": (
        "Expected Value. The average profit per dollar bet if you could make "
        "this bet thousands of times. EV = (probability x odds) - 1. Positive "
        "EV means the bet is profitable in the long run."
    ),
    "overlay": (
        "How much the model's probability exceeds the market's implied probability. "
        "An overlay of 1.5x means the model thinks the horse is 50% more likely "
        "to win than the odds suggest."
    ),
    "implied_prob": (
        "The win probability implied by the betting odds. For 4-1 odds, the "
        "implied probability is 1/(4+1) = 20%. If the model's probability is "
        "higher, there may be value."
    ),
    "kelly_pct": (
        "Kelly-optimal bet size as a percentage of bankroll. Calculated from the "
        "model's edge and the odds. Higher Kelly % = stronger edge = larger bet."
    ),
    "fractional_kelly": (
        "Fraction of the full Kelly bet to actually wager. Using 0.25 (quarter "
        "Kelly) means betting 25% of the theoretically optimal amount, which "
        "greatly reduces variance."
    ),
    "min_ev_threshold": (
        "Minimum expected value required to place a bet. A threshold of 0.08 "
        "means only bets with 8%+ expected profit qualify."
    ),
    "min_overlay": (
        "Minimum overlay ratio required. A value of 1.20 means the model's "
        "probability must be at least 20% higher than what the odds imply."
    ),
}

# --- Strategy Descriptions ---
STRATEGIES = {
    "Flat Bet": (
        "Wagers the same fixed dollar amount on every qualifying bet regardless "
        "of edge size or bankroll. Simple and easy to track."
    ),
    "Kelly Criterion": (
        "Sizes bets proportionally to the estimated edge. Larger edges get larger "
        "bets. Mathematically optimal for long-term bankroll growth, but can be "
        "volatile at full Kelly."
    ),
    "Value Betting": (
        "Only bets when the model finds a minimum edge (overlay) between its "
        "probability and the market odds. Uses fixed fractional sizing."
    ),
    "Top Pick": (
        "Bets a fixed amount only on the model's top-ranked horse in each race, "
        "if it meets the minimum probability threshold."
    ),
    "Momentum": (
        "Adjusts bet size based on recent results. Increases stakes during winning "
        "streaks and decreases during losing streaks."
    ),
    "Morning Favorite": (
        "Targets horses that are morning-line favorites but where the model finds "
        "additional edge beyond what the odds suggest."
    ),
}
