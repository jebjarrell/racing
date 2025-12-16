# Configuration Guide

**Version:** 1.0
**Last Updated:** 2025-12-16

---

## Table of Contents

1. [Overview](#1-overview)
2. [Configuration File Structure](#2-configuration-file-structure)
3. [Betting Parameters](#3-betting-parameters)
4. [Bankroll Settings](#4-bankroll-settings)
5. [Track Classifications](#5-track-classifications)
6. [Split Testing Configuration](#6-split-testing-configuration)
7. [Model Parameters](#7-model-parameters)
8. [Database Configuration](#8-database-configuration)
9. [Environment Variables](#9-environment-variables)
10. [Tuning Guidelines](#10-tuning-guidelines)

---

## 1. Overview

### 1.1 Configuration Files

| File | Purpose |
|------|---------|
| `config/config.yaml` | Main application configuration |
| `config/database.py` | Database connection settings |
| `config/logging.py` | Logging configuration |
| `.env` | Environment-specific secrets |

### 1.2 Configuration Priority

1. Environment variables (highest priority)
2. Command-line arguments
3. `config.yaml` file
4. Default values (lowest priority)

---

## 2. Configuration File Structure

The main configuration file (`config/config.yaml`) is organized into sections:

```yaml
# Main Configuration File
# config/config.yaml

betting:           # Betting strategy parameters
bankroll:          # Bankroll management
tracks:            # Track classifications
split_test:        # A/B testing configuration
sensitivity:       # Sensitivity analysis
model:             # ML model parameters
features:          # Feature engineering
database:          # Database connection
logging:           # Logging settings
dashboard:         # Dashboard configuration
```

---

## 3. Betting Parameters

### 3.1 Core Betting Settings

```yaml
betting:
  # Kelly Criterion
  fractional_kelly: 0.25          # Use quarter Kelly (0.25 = 25%)
                                  # Range: 0.10 - 0.50
                                  # Lower = more conservative

  # Bet Selection Filters
  min_ev_threshold: 0.08          # Minimum expected value (8%)
                                  # Range: 0.05 - 0.15

  min_probability: 0.08           # Minimum model probability (8%)
                                  # Range: 0.05 - 0.15

  min_overlay: 1.20               # Minimum overlay ratio (20% edge)
                                  # Range: 1.10 - 1.50
                                  # 1.20 means model prob >= 1.2x market prob

  max_odds: 15.0                  # Maximum odds to bet (15-1)
                                  # Range: 10.0 - 30.0

  # Risk Limits
  max_per_race_pct: 0.02          # Maximum per-race exposure (2%)
                                  # Range: 0.01 - 0.05

  daily_loss_limit_pct: 0.10      # Daily stop-loss (10%)
                                  # Range: 0.05 - 0.15

  min_bet_amount: 2.0             # Minimum bet size in dollars
                                  # Platform minimum is typically $2
```

### 3.2 Parameter Descriptions

| Parameter | Default | Description |
|-----------|---------|-------------|
| `fractional_kelly` | 0.25 | Fraction of full Kelly to bet. Lower values reduce variance. |
| `min_ev_threshold` | 0.08 | Minimum EV required. Higher = fewer, stronger bets. |
| `min_probability` | 0.08 | Avoid extreme longshots. Higher = more favorites. |
| `min_overlay` | 1.20 | Minimum edge over market. Higher = stronger conviction. |
| `max_odds` | 15.0 | Maximum odds. Lower = avoid lottery tickets. |
| `max_per_race_pct` | 0.02 | Single-race exposure limit. |
| `daily_loss_limit_pct` | 0.10 | Daily stop-loss trigger. |
| `min_bet_amount` | 2.0 | Platform minimum bet. |

---

## 4. Bankroll Settings

```yaml
bankroll:
  initial: 2000.0                 # Starting bankroll in USD
  currency: USD                   # Currency code

  # Bankroll adjustment triggers
  recalculation_frequency: daily  # When to recalculate stake sizes
                                  # Options: daily, weekly, fixed

  # Drawdown protection
  reduce_stakes_threshold: 0.20   # Reduce to 50% stakes if down 20%
  pause_threshold: 0.30           # Pause betting if down 30%

  # Growth handling
  increase_stakes_threshold: 0.25 # Increase stakes if up 25%
  max_stake_multiplier: 2.0       # Maximum stake increase factor
```

### 4.1 Bankroll Management Rules

| Condition | Action |
|-----------|--------|
| Bankroll down 20% | Reduce Kelly fraction to 0.125 |
| Bankroll down 30% | Pause betting, review model |
| Bankroll up 25% | Optionally increase Kelly to 0.30 |
| Daily loss > 10% | Stop betting for the day |

---

## 5. Track Classifications

```yaml
tracks:
  high_volume:
    # Major US tracks with high liquidity
    - CD    # Churchill Downs
    - SAR   # Saratoga
    - BEL   # Belmont
    - GP    # Gulfstream Park
    - SA    # Santa Anita
    - DMR   # Del Mar
    - KEE   # Keeneland
    - AQU   # Aqueduct

  regional:
    # Regional tracks with potentially less efficient markets
    - TP    # Turfway Park
    - CT    # Charles Town
    - PEN   # Penn National
    - LRL   # Laurel Park
    - TAM   # Tampa Bay Downs
    - FG    # Fair Grounds
    - OP    # Oaklawn Park
    - GG    # Golden Gate Fields

  excluded:
    # Tracks to exclude from betting
    - []    # Add any tracks to exclude
```

### 5.1 Track Code Reference

| Code | Track Name | Location | Type |
|------|------------|----------|------|
| CD | Churchill Downs | Louisville, KY | High Volume |
| SAR | Saratoga | Saratoga Springs, NY | High Volume |
| BEL | Belmont | Elmont, NY | High Volume |
| GP | Gulfstream Park | Hallandale Beach, FL | High Volume |
| SA | Santa Anita | Arcadia, CA | High Volume |
| DMR | Del Mar | Del Mar, CA | High Volume |
| KEE | Keeneland | Lexington, KY | High Volume |
| AQU | Aqueduct | Ozone Park, NY | High Volume |
| TP | Turfway Park | Florence, KY | Regional |
| CT | Charles Town | Charles Town, WV | Regional |
| PEN | Penn National | Grantville, PA | Regional |
| LRL | Laurel Park | Laurel, MD | Regional |

---

## 6. Split Testing Configuration

```yaml
split_test:
  enabled: true                   # Enable A/B testing

  allocation:
    high_volume: 0.50             # 50% of bets on high-volume tracks
    regional: 0.50                # 50% of bets on regional tracks

  min_bets_for_significance: 500  # Minimum bets per segment
                                  # Before making allocation decisions

  evaluation_period_days: 90      # Days before evaluating results

  significance_level: 0.05        # p-value threshold for decisions

  # Decision thresholds
  roi_difference_threshold: 0.02  # 2% ROI difference to shift allocation
  shift_amount: 0.20              # Shift 20% when significant diff found
```

### 6.1 Split Test Decision Matrix

| Condition | Criteria | Action |
|-----------|----------|--------|
| Regional wins | ROI diff > 2%, p < 0.05 | Shift to 70/30 regional |
| High-volume wins | ROI diff > 2%, p < 0.05 | Shift to 70/30 high-volume |
| No difference | p > 0.05 | Maintain 50/50 |
| Insufficient data | < 500 bets per segment | Continue 50/50, wait |

---

## 7. Model Parameters

```yaml
model:
  # LightGBM configuration
  algorithm: lightgbm

  hyperparameters:
    n_estimators: 500
    max_depth: 6
    learning_rate: 0.05
    subsample: 0.8
    colsample_bytree: 0.8
    reg_alpha: 0.1
    reg_lambda: 0.1
    min_child_samples: 20

  # Training configuration
  early_stopping_rounds: 50
  random_state: 42
  n_jobs: -1

  # Calibration
  calibration_method: isotonic    # isotonic or platt
  field_size_stratification: true

  # Data splits
  splits:
    train:
      start: "2023-01-01"
      end: "2023-06-30"
    validation:
      start: "2023-07-01"
      end: "2023-09-30"
    test:
      start: "2023-10-01"
      end: "2023-12-31"
```

---

## 8. Database Configuration

```yaml
database:
  type: postgresql                # postgresql or sqlite

  postgresql:
    host: localhost
    port: 5432
    database: racing_db
    schema: racing
    pool_size: 10
    max_overflow: 20

  sqlite:
    path: racing_data.db          # For development/testing

  timescaledb:
    enabled: true
    chunk_time_interval: 7 days
```

---

## 9. Environment Variables

Create a `.env` file for sensitive configuration:

```bash
# Database credentials
DB_HOST=localhost
DB_PORT=5432
DB_NAME=racing_db
DB_USER=racing_user
DB_PASSWORD=your_secure_password

# API keys (if applicable)
TWINSPIRES_API_KEY=your_api_key
DRAFTKINGS_API_KEY=your_api_key

# MLflow tracking
MLFLOW_TRACKING_URI=http://localhost:5000
MLFLOW_EXPERIMENT_NAME=racing_model

# Environment
ENVIRONMENT=development  # development, staging, production

# Logging
LOG_LEVEL=INFO
```

### 9.1 Loading Environment Variables

```python
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PASSWORD = os.getenv('DB_PASSWORD')
```

---

## 10. Tuning Guidelines

### 10.1 Conservative vs Aggressive Settings

| Parameter | Conservative | Moderate | Aggressive |
|-----------|--------------|----------|------------|
| fractional_kelly | 0.15 | 0.25 | 0.40 |
| min_ev_threshold | 0.12 | 0.08 | 0.05 |
| min_overlay | 1.30 | 1.20 | 1.10 |
| max_per_race_pct | 0.01 | 0.02 | 0.03 |

### 10.2 When to Adjust Parameters

| Scenario | Adjustment |
|----------|------------|
| Bankroll < $1,000 | Reduce Kelly to 0.15 |
| Consistent losses (30-day) | Increase min_ev to 0.12 |
| High win rate, low ROI | Reduce max_odds |
| Model recalibrated | Reset to defaults, monitor |
| Regional outperforming | Shift allocation to 70/30 |

### 10.3 Seasonal Considerations

| Season | Consideration |
|--------|---------------|
| Winter (Jan-Mar) | Many tracks closed, focus on FL/CA |
| Spring (Apr-Jun) | Derby season, high volume |
| Summer (Jul-Sep) | Saratoga, Del Mar meets |
| Fall (Oct-Dec) | Breeders' Cup, end of year |

---

## Appendix A: Full Configuration Template

```yaml
# config/config.yaml - Full Template

betting:
  fractional_kelly: 0.25
  min_ev_threshold: 0.08
  min_probability: 0.08
  min_overlay: 1.20
  max_odds: 15.0
  max_per_race_pct: 0.02
  daily_loss_limit_pct: 0.10
  min_bet_amount: 2.0

bankroll:
  initial: 2000.0
  currency: USD
  recalculation_frequency: daily
  reduce_stakes_threshold: 0.20
  pause_threshold: 0.30

tracks:
  high_volume:
    - CD
    - SAR
    - BEL
    - GP
    - SA
    - DMR
  regional:
    - KEE
    - TP
    - CT
    - PEN
    - LRL
    - TAM
  excluded: []

split_test:
  enabled: true
  allocation:
    high_volume: 0.50
    regional: 0.50
  min_bets_for_significance: 500
  significance_level: 0.05

sensitivity:
  edge_degradation_scenarios:
    best_case: 0.20
    expected: 0.35
    worst_case: 0.55

model:
  algorithm: lightgbm
  hyperparameters:
    n_estimators: 500
    max_depth: 6
    learning_rate: 0.05
  calibration_method: isotonic

database:
  type: postgresql
  postgresql:
    host: ${DB_HOST}
    port: ${DB_PORT}
    database: ${DB_NAME}

logging:
  level: INFO
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

dashboard:
  host: 0.0.0.0
  port: 8501
  refresh_interval: 60
```

---

*Document maintained by: DevOps Team*
*Review cycle: Quarterly or after major changes*
