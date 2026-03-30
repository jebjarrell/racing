# Reliability Refactoring: Centralize Repeated Patterns

## Problem

The Streamlit dashboard has 13 repeated patterns across 12 files. During bug-fixing, the same fix had to be applied in multiple places (odds conversion, DB connections, path resolution, error handling). This duplication is the root cause of inconsistency — when a pattern exists in one place, bugs get fixed once. When it's copied across 6 pages, bugs persist in the copies.

## Scope

Full consolidation: extract all repeated patterns into shared utilities and components, then update every page to use them.

## New Files

### `app/utils/__init__.py`

Empty init file for the utils package.

### `app/utils/db.py` — Database Access

Single module for all SQLite interactions. Eliminates 9+ raw `sqlite3.connect()` calls spread across 4 pages.

```python
from contextlib import contextmanager

@contextmanager
def get_connection(db_path=None):
    """Context manager that opens and closes a SQLite connection.
    Defaults to PROJECT_ROOT / 'racing_data.db'."""

def query_single(sql, params=None, db_path=None):
    """Execute query, return single scalar value. Opens/closes its own connection."""

def query_df(sql, params=None, db_path=None):
    """Execute query, return pandas DataFrame. Opens/closes its own connection."""

def db_path_default():
    """Return the default DB path string."""

def db_exists():
    """Return True if the default DB file exists."""

@contextmanager
def streamlit_error_boundary(operation_name):
    """Context manager that catches exceptions and displays them via st.error.
    Usage:
        with streamlit_error_boundary("Training"):
            ... risky code ...
    Shows user-friendly error + traceback in expander."""
```

### `app/utils/betting.py` — Betting Math

All odds conversion and betting calculations. Replaces duplicated logic in `5_Race_Predictions.py` (30+ lines) and `backtesting/backtester.py::_to_decimal_odds`.

```python
def to_decimal_odds(odds_value):
    """Convert any odds format to decimal odds.
    Handles: to-one (5.0 -> 6.0), American (+350 -> 4.5, -200 -> 1.5),
    None/invalid (-> 2.0 fallback). Casts strings to float."""

def calculate_metrics(prob, decimal_odds, kelly_fraction=0.25, max_per_race=0.02, bankroll=1000):
    """Return dict with ev, implied_prob, overlay, kelly, stake."""

def qualifies_for_bet(ev, prob, overlay, decimal_odds, kelly, config):
    """Return True if all betting filters pass (min_ev, min_prob, min_overlay, max_odds, kelly > 0)."""
```

### `app/utils/features.py` — Feature Preparation

Replaces duplicated feature column filtering in Training and Predictions pages.

```python
def prepare_feature_matrix(df, feature_columns):
    """Select available columns, add missing as 0, reorder to match training order, fillna(0).
    Returns DataFrame with exactly feature_columns in order."""

def get_field_sizes(df, default=8):
    """Extract field_size column from df with fallback to default. Returns numpy array."""
```

### `app/components/model_selector.py` — Model Version Widget

Replaces identical 6-line blocks in Backtesting and Predictions.

```python
def select_model(models, require_features=False):
    """Render model version selectbox. Returns selected model info dict.
    Calls st.stop() if no models available or selection invalid.
    If require_features=True, also checks for non-empty feature_columns."""
```

### `app/components/metrics_display.py` — Metric Rendering

Replaces identical metric display blocks in Dashboard, Training, and Sidebar.

```python
def display_model_metrics(metrics, show_features=False, feature_count=0):
    """Render ROI-AUC, Brier, ECE, Log Loss metrics in columns.
    Optionally show feature count."""

def display_backtest_summary(results):
    """Render ROI, Profit, Total Bets, Win Rate, Avg Odds, Max Drawdown in columns."""
```

## Page Changes

### `app/pages/1_Dashboard.py`

- Remove `sqlite3` import and all raw connection code.
- `get_db_stats()`: Replace with calls to `db.query_single()` per stat, each in its own try/except.
- `get_recent_races()`: Replace with `db.query_df()`.
- `get_top_performers()`: Replace with `db.query_df()`.
- Model metrics display: Replace inline metrics with `metrics_display.display_model_metrics()`.

### `app/pages/2_Data_Management.py`

- Remove `sqlite3` import and `_query_tables()` / `_run_browser_query()` helper functions.
- Replace all DB access with `db.get_connection()` and `db.query_df()`.
- Standardize path usage to always use `Path` objects from `PROJECT_ROOT`.

### `app/pages/3_Model_Training.py`

- Replace feature column filtering with `features.prepare_feature_matrix()`.
- Replace field size extraction with `features.get_field_sizes()`.
- Replace results metric display with `metrics_display.display_model_metrics()`.
- Replace try/except/traceback with `streamlit_error_boundary`.
- Use `db.db_path_default()` for pipeline instantiation.

### `app/pages/4_Backtesting.py`

- Replace model selection block with `model_selector.select_model()`.
- Replace `load_model_and_backtester()` DB_PATH with `db.db_path_default()`.
- Replace results metric display with `metrics_display.display_backtest_summary()`.
- Replace try/except/traceback with `streamlit_error_boundary`.

### `app/pages/5_Race_Predictions.py`

- Replace model selection block with `model_selector.select_model(require_features=True)`.
- Replace DB access with `db.get_connection()` / `db.query_df()`.
- Replace odds conversion loop (~30 lines) with `betting.to_decimal_odds()` + `betting.calculate_metrics()`.
- Replace qualifying check with `betting.qualifies_for_bet()`.
- Replace feature preparation with `features.prepare_feature_matrix()`.
- Replace try/except/traceback with `streamlit_error_boundary`.

### `app/pages/6_Settings.py`

No significant changes — this page doesn't use DB or model loading.

### `backtesting/backtester.py`

- Remove `_to_decimal_odds()` method.
- Import and use `app.utils.betting.to_decimal_odds()` instead.

### `app/components/sidebar.py`

- Replace inline metric rendering in `render_sidebar()` with `metrics_display.display_model_metrics()`.
- Export `db_path_default` concept (already exports `PROJECT_ROOT`).

## What Does NOT Change

- No behavioral changes to any page. Every page produces the same output as before.
- No changes to backend modules (`models/`, `features/`, `backtesting/strategies.py`).
- No changes to config format or DB schema.
- No changes to `app/components/charts.py` or `app/components/tooltips.py`.

## File Impact Summary

| Action | File |
|--------|------|
| CREATE | `app/utils/__init__.py` |
| CREATE | `app/utils/db.py` |
| CREATE | `app/utils/betting.py` |
| CREATE | `app/utils/features.py` |
| CREATE | `app/components/model_selector.py` |
| CREATE | `app/components/metrics_display.py` |
| MODIFY | `app/pages/1_Dashboard.py` |
| MODIFY | `app/pages/2_Data_Management.py` |
| MODIFY | `app/pages/3_Model_Training.py` |
| MODIFY | `app/pages/4_Backtesting.py` |
| MODIFY | `app/pages/5_Race_Predictions.py` |
| MODIFY | `app/components/sidebar.py` |
| MODIFY | `backtesting/backtester.py` |

6 new files, 7 modified files.

## Verification

1. All files compile (`py_compile`)
2. `streamlit run app/app.py` — all 6 pages load without errors
3. Dashboard shows stats, models, recent races
4. Predictions page produces identical results to before refactoring
5. Backtesting page runs all strategies including Morning Favorite
6. Settings save/load round-trips correctly
7. No raw `sqlite3.connect()` calls remain in `app/pages/` (grep verification)
8. No `_to_decimal_odds` method remains in backtester (grep verification)
