# Reliability Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Centralize 13 repeated patterns across the dashboard into shared utilities and components, eliminating duplication that caused bugs.

**Architecture:** Extract DB access, betting math, feature preparation, model selection, and metric display into `app/utils/` and `app/components/`. Every page imports from these instead of reimplementing. No behavioral changes — same outputs, fewer places for bugs to hide.

**Tech Stack:** Python, Streamlit, SQLite3, pandas, numpy, plotly

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| CREATE | `app/utils/__init__.py` | Package init |
| CREATE | `app/utils/db.py` | DB connections, queries, error boundary |
| CREATE | `app/utils/betting.py` | Odds conversion, EV/overlay/Kelly math |
| CREATE | `app/utils/features.py` | Feature matrix preparation, field sizes |
| CREATE | `app/components/model_selector.py` | Model version selectbox widget |
| CREATE | `app/components/metrics_display.py` | Reusable metric rendering |
| MODIFY | `app/pages/1_Dashboard.py` | Use db utils, metrics_display |
| MODIFY | `app/pages/2_Data_Management.py` | Use db utils |
| MODIFY | `app/pages/3_Model_Training.py` | Use features, metrics_display, error boundary |
| MODIFY | `app/pages/4_Backtesting.py` | Use model_selector, metrics_display, error boundary |
| MODIFY | `app/pages/5_Race_Predictions.py` | Use all utils: db, betting, features, model_selector, error boundary |
| MODIFY | `app/components/sidebar.py` | Use metrics_display |
| MODIFY | `backtesting/backtester.py` | Use betting.to_decimal_odds |

---

### Task 1: Create `app/utils/db.py`

**Files:**
- Create: `app/utils/__init__.py`
- Create: `app/utils/db.py`

- [ ] **Step 1: Create the utils package and db module**

```python
# app/utils/__init__.py
# (empty)
```

```python
# app/utils/db.py
"""Centralized database access for the Streamlit dashboard."""

import os
import sqlite3
from contextlib import contextmanager

import pandas as pd
import streamlit as st

from app.components.sidebar import PROJECT_ROOT

_DEFAULT_DB = str(PROJECT_ROOT / "racing_data.db")


def db_path_default() -> str:
    """Return the default database path."""
    return _DEFAULT_DB


def db_exists(db_path: str = None) -> bool:
    """Return True if the database file exists."""
    return os.path.exists(db_path or _DEFAULT_DB)


@contextmanager
def get_connection(db_path: str = None):
    """Context manager that opens and closes a SQLite connection.

    Usage:
        with get_connection() as conn:
            conn.execute("SELECT ...")
    """
    conn = sqlite3.connect(db_path or _DEFAULT_DB)
    try:
        yield conn
    finally:
        conn.close()


def query_single(sql: str, params=None, db_path: str = None):
    """Execute query, return single scalar value."""
    with get_connection(db_path) as conn:
        row = conn.execute(sql, params or []).fetchone()
        return row[0] if row else None


def query_df(sql: str, params=None, db_path: str = None) -> pd.DataFrame:
    """Execute query, return pandas DataFrame."""
    with get_connection(db_path) as conn:
        return pd.read_sql_query(sql, conn, params=params or [])


@contextmanager
def streamlit_error_boundary(operation_name: str):
    """Context manager that catches exceptions and displays them via Streamlit.

    Usage:
        with streamlit_error_boundary("Training"):
            ... risky code ...
    """
    try:
        yield
    except Exception as e:
        st.error(f"{operation_name} failed: {e}")
        import traceback
        with st.expander("Technical details"):
            st.code(traceback.format_exc())
```

- [ ] **Step 2: Verify it compiles**

Run: `cd F:/racing && python -c "import py_compile; py_compile.compile('app/utils/db.py', doraise=True); print('OK')"`

- [ ] **Step 3: Verify imports work**

Run: `cd F:/racing && python -c "from app.utils.db import get_connection, query_single, query_df, db_path_default, db_exists, streamlit_error_boundary; print('All imports OK')"`

- [ ] **Step 4: Commit**

```bash
git add app/utils/__init__.py app/utils/db.py
git commit -m "feat: add centralized DB access module (app/utils/db.py)"
```

---

### Task 2: Create `app/utils/betting.py`

**Files:**
- Create: `app/utils/betting.py`

- [ ] **Step 1: Create the betting math module**

```python
# app/utils/betting.py
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
    # Coerce to float
    try:
        odds = float(odds_value) if pd.notna(odds_value) else None
    except (ValueError, TypeError):
        return 2.0

    if odds is None or odds <= 0:
        return 2.0
    elif odds >= 100:
        return (odds / 100) + 1  # American positive
    elif odds <= -100:
        return (100 / abs(odds)) + 1  # American negative
    else:
        return odds + 1  # To-one format


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
        and decimal_odds <= max_odds + 1  # max_odds is to-one, decimal is +1
        and kelly > 0
    )
```

- [ ] **Step 2: Verify it compiles and imports**

Run: `cd F:/racing && python -c "from app.utils.betting import to_decimal_odds, calculate_metrics, qualifies_for_bet; print(to_decimal_odds(5.0), to_decimal_odds(350), to_decimal_odds(-200), to_decimal_odds(None))"`
Expected: `6.0 4.5 1.5 2.0`

- [ ] **Step 3: Commit**

```bash
git add app/utils/betting.py
git commit -m "feat: add centralized betting math module (app/utils/betting.py)"
```

---

### Task 3: Create `app/utils/features.py`

**Files:**
- Create: `app/utils/features.py`

- [ ] **Step 1: Create the feature preparation module**

```python
# app/utils/features.py
"""Centralized feature matrix preparation."""

import numpy as np
import pandas as pd


def prepare_feature_matrix(df: pd.DataFrame, feature_columns: list) -> pd.DataFrame:
    """Select available columns, add missing as 0, reorder to match training order.

    Args:
        df: Raw features DataFrame
        feature_columns: Ordered list of expected feature column names

    Returns:
        DataFrame with exactly feature_columns in order, NaN filled with 0.
    """
    available = [c for c in feature_columns if c in df.columns]
    X = df[available].copy()
    for c in feature_columns:
        if c not in X.columns:
            X[c] = 0
    return X[feature_columns].fillna(0)


def get_field_sizes(df: pd.DataFrame, default: int = 8) -> np.ndarray:
    """Extract field_size column from df with fallback to default.

    Returns numpy array of field sizes.
    """
    if "field_size" in df.columns:
        return df["field_size"].fillna(default).values
    return np.full(len(df), default)
```

- [ ] **Step 2: Verify it compiles and imports**

Run: `cd F:/racing && python -c "from app.utils.features import prepare_feature_matrix, get_field_sizes; import pandas as pd; df = pd.DataFrame({'a': [1,2], 'b': [3,4]}); X = prepare_feature_matrix(df, ['a','b','c']); print(X.columns.tolist(), X['c'].tolist())"`
Expected: `['a', 'b', 'c'] [0, 0]`

- [ ] **Step 3: Commit**

```bash
git add app/utils/features.py
git commit -m "feat: add feature matrix preparation module (app/utils/features.py)"
```

---

### Task 4: Create `app/components/model_selector.py`

**Files:**
- Create: `app/components/model_selector.py`

- [ ] **Step 1: Create the model selector component**

```python
# app/components/model_selector.py
"""Reusable model version selector widget."""

import streamlit as st


def select_model(models: list, require_features: bool = False) -> dict:
    """Render model version selectbox and return selected model info.

    Args:
        models: List of model info dicts from get_available_models()
        require_features: If True, also check for non-empty feature_columns

    Returns:
        Selected model info dict. Calls st.stop() if selection is invalid.
    """
    if not models:
        st.warning("No trained model found. Train one in **Model Training** first.")
        st.stop()

    model_versions = [m["version"] for m in models]
    selected_version = st.selectbox("Model Version", model_versions)
    selected = next((m for m in models if m["version"] == selected_version), None)

    if selected is None:
        st.error(f"Model version '{selected_version}' not found.")
        st.stop()

    if require_features and not selected.get("feature_columns"):
        st.error("Model metadata is missing `feature_columns`. Retrain the model to fix this.")
        st.stop()

    return selected
```

- [ ] **Step 2: Verify it compiles**

Run: `cd F:/racing && python -c "import py_compile; py_compile.compile('app/components/model_selector.py', doraise=True); print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add app/components/model_selector.py
git commit -m "feat: add reusable model version selector component"
```

---

### Task 5: Create `app/components/metrics_display.py`

**Files:**
- Create: `app/components/metrics_display.py`

- [ ] **Step 1: Create the metrics display component**

```python
# app/components/metrics_display.py
"""Reusable metric display widgets for model and backtest results."""

import streamlit as st

from app.components.tooltips import METRICS, BETTING


def display_model_metrics(metrics: dict, show_features: bool = False, feature_count: int = 0):
    """Render model evaluation metrics in columns.

    Args:
        metrics: Dict with keys roc_auc, brier_score, ece, log_loss
        show_features: If True, show feature count as a 5th column
        feature_count: Number of features (only used if show_features=True)
    """
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
    """Render backtest summary metrics in 6 columns.

    Args:
        results: BacktestResult object with roi, profit, total_bets, win_rate, avg_odds_bet, max_drawdown
    """
    mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
    mc1.metric("ROI", f"{results.roi:+.2%}", help=BETTING["roi"])
    mc2.metric("Profit", f"${results.profit:+,.2f}")
    mc3.metric("Total Bets", f"{results.total_bets:,}")
    mc4.metric("Win Rate", f"{results.win_rate:.1%}", help=BETTING["win_rate"])
    mc5.metric("Avg Odds", f"{results.avg_odds_bet:.1f}", help=BETTING["avg_odds"])
    mc6.metric("Max Drawdown", f"{results.max_drawdown:.1%}", help=BETTING["max_drawdown"])
```

- [ ] **Step 2: Verify it compiles**

Run: `cd F:/racing && python -c "import py_compile; py_compile.compile('app/components/metrics_display.py', doraise=True); print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add app/components/metrics_display.py
git commit -m "feat: add reusable metrics display components"
```

---

### Task 6: Refactor `app/pages/1_Dashboard.py`

**Files:**
- Modify: `app/pages/1_Dashboard.py`

- [ ] **Step 1: Replace all raw DB access with db utils**

Replace `sqlite3` import and all three query functions (`get_db_stats`, `get_recent_races`, `get_top_performers`) to use `app.utils.db`. Replace the inline model metrics display (lines 176-184) with `metrics_display.display_model_metrics()`.

Key changes:
- Remove `import sqlite3`
- Add `from app.utils.db import query_single, query_df, db_exists, db_path_default`
- Add `from app.components.metrics_display import display_model_metrics`
- `get_db_stats()`: Use `query_single()` per stat, each in its own try/except
- `get_recent_races()`: Use `query_df()`
- `get_top_performers()`: Use `query_df()`
- Model expanders: Replace 4 inline `st.metric()` calls with `display_model_metrics(metrics)`
- Remove `DB_PATH = str(PROJECT_ROOT / "racing_data.db")` — use `db_path_default()` or `db_exists()`

- [ ] **Step 2: Verify page compiles**

Run: `cd F:/racing && python -c "import py_compile; py_compile.compile('app/pages/1_Dashboard.py', doraise=True); print('OK')"`

- [ ] **Step 3: Verify no raw sqlite3 usage remains**

Run: `cd F:/racing && grep -n "sqlite3" app/pages/1_Dashboard.py` — expected: no output

- [ ] **Step 4: Commit**

```bash
git add app/pages/1_Dashboard.py
git commit -m "refactor: Dashboard uses centralized db utils and metrics display"
```

---

### Task 7: Refactor `app/pages/2_Data_Management.py`

**Files:**
- Modify: `app/pages/2_Data_Management.py`

- [ ] **Step 1: Replace all raw DB access with db utils**

Key changes:
- Remove `import sqlite3`
- Add `from app.utils.db import get_connection, query_df, db_exists, db_path_default`
- Replace `_query_tables()` with `query_df("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")`  and extract the column
- Replace `_run_browser_query()` with `query_df()` calls
- Replace `conn_tmp = sqlite3.connect(DB_PATH)` filter block with `query_single()` calls
- Remove `DB_PATH` constant — use `db_path_default()`
- Standardize paths: remove the `PROJECT_ROOT = str(_PROJECT_ROOT)` pattern, import `PROJECT_ROOT` directly as a `Path`

- [ ] **Step 2: Verify page compiles**

Run: `cd F:/racing && python -c "import py_compile; py_compile.compile('app/pages/2_Data_Management.py', doraise=True); print('OK')"`

- [ ] **Step 3: Verify no raw sqlite3 usage remains**

Run: `cd F:/racing && grep -n "sqlite3" app/pages/2_Data_Management.py` — expected: no output

- [ ] **Step 4: Commit**

```bash
git add app/pages/2_Data_Management.py
git commit -m "refactor: Data Management uses centralized db utils"
```

---

### Task 8: Refactor `app/pages/3_Model_Training.py`

**Files:**
- Modify: `app/pages/3_Model_Training.py`

- [ ] **Step 1: Replace feature prep, field sizes, metrics display, and error handling**

Key changes:
- Add `from app.utils.features import prepare_feature_matrix, get_field_sizes`
- Add `from app.utils.db import streamlit_error_boundary, db_path_default`
- Add `from app.components.metrics_display import display_model_metrics`
- Replace feature_cols filtering (line 147) and X_train/X_val/X_test construction with:
  ```python
  feature_cols = [c for c in FEATURE_COLUMNS if c in train_df.columns]
  X_train = prepare_feature_matrix(train_df, feature_cols)
  y_train = train_df["is_winner"]
  X_val = prepare_feature_matrix(val_df, feature_cols)
  y_val = val_df["is_winner"]
  X_test = prepare_feature_matrix(test_df, feature_cols)
  y_test = test_df["is_winner"]
  ```
- Replace field_size extraction (lines 175, 184) with:
  ```python
  val_field_sizes = get_field_sizes(val_df)
  test_field_sizes = get_field_sizes(test_df)
  ```
- Replace 5-metric display (lines 232-237) with:
  ```python
  display_model_metrics(
      {"roc_auc": roc_auc, "brier_score": brier, "ece": cal_metrics["ece"], "log_loss": logloss},
      show_features=True, feature_count=len(feature_cols),
  )
  ```
- Replace outer try/except/traceback (lines 264-267) with `streamlit_error_boundary("Training")`
- Replace `db_path="racing_data.db"` in pipeline instantiation with `db_path_default()`

- [ ] **Step 2: Verify page compiles**

Run: `cd F:/racing && python -c "import py_compile; py_compile.compile('app/pages/3_Model_Training.py', doraise=True); print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add app/pages/3_Model_Training.py
git commit -m "refactor: Training page uses shared features, metrics, error boundary"
```

---

### Task 9: Refactor `app/pages/4_Backtesting.py`

**Files:**
- Modify: `app/pages/4_Backtesting.py`

- [ ] **Step 1: Replace model selection, metrics display, and error handling**

Key changes:
- Add `from app.components.model_selector import select_model`
- Add `from app.components.metrics_display import display_backtest_summary`
- Add `from app.utils.db import streamlit_error_boundary, db_path_default`
- Replace model selection block (lines 37-42) with:
  ```python
  selected_model_info = select_model(models)
  ```
- In `load_model_and_backtester()`, replace `db_path=DB_PATH` with `db_path=db_path_default()`
- Remove `DB_PATH` constant
- Replace metric display in `display_results()` (lines 148-154) with:
  ```python
  display_backtest_summary(results)
  ```
- Replace both try/except/traceback blocks with `streamlit_error_boundary`:
  ```python
  with streamlit_error_boundary("Backtest"):
      backtester = load_model_and_backtester()
      results = backtester.run(...)
      display_results(results)
  ```

- [ ] **Step 2: Verify page compiles**

Run: `cd F:/racing && python -c "import py_compile; py_compile.compile('app/pages/4_Backtesting.py', doraise=True); print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add app/pages/4_Backtesting.py
git commit -m "refactor: Backtesting uses model_selector, metrics_display, error boundary"
```

---

### Task 10: Refactor `app/pages/5_Race_Predictions.py`

**Files:**
- Modify: `app/pages/5_Race_Predictions.py`

This is the largest refactoring — touches every pattern.

- [ ] **Step 1: Replace model selection, DB access, odds conversion, betting math, features, error handling**

Key changes:
- Add imports:
  ```python
  from app.components.model_selector import select_model
  from app.utils.db import get_connection, query_df, db_exists, db_path_default, streamlit_error_boundary
  from app.utils.betting import to_decimal_odds, calculate_metrics, qualifies_for_bet
  from app.utils.features import prepare_feature_matrix
  ```
- Remove `import sqlite3`, remove `DB_PATH` constant
- Replace model selection block (lines 44-53) with:
  ```python
  selected_model_info = select_model(models, require_features=True)
  ```
- Replace `with sqlite3.connect(DB_PATH) as conn:` blocks with `with get_connection() as conn:` or `query_df()`
- Replace odds coercion + conversion block (lines 220-247) with:
  ```python
  ml_odds = r.get("morning_line_odds") or r.get("morning_line_odds_entry")
  actual = r.get("actual_odds")
  odds_to_use = to_decimal_odds(actual) if pd.notna(actual) and float(actual) > 0 else to_decimal_odds(ml_odds)
  # Wait — we need the raw value to pick, then convert. Adjust:
  raw_actual = actual
  try:
      raw_actual = float(actual) if pd.notna(actual) else None
  except (ValueError, TypeError):
      raw_actual = None
  try:
      raw_ml = float(ml_odds) if pd.notna(ml_odds) else None
  except (ValueError, TypeError):
      raw_ml = None
  raw_odds = raw_actual if raw_actual and raw_actual > 0 else raw_ml
  decimal_odds = to_decimal_odds(raw_odds)
  ```
  Actually, simplify — `to_decimal_odds` already handles None/NaN/string. So:
  ```python
  raw_actual = r.get("actual_odds")
  raw_ml = r.get("morning_line_odds") or r.get("morning_line_odds_entry")
  # Prefer actual odds; fall back to morning line
  try:
      actual_val = float(raw_actual) if pd.notna(raw_actual) else None
  except (ValueError, TypeError):
      actual_val = None
  odds_source = actual_val if actual_val and actual_val > 0 else raw_ml
  decimal_odds = to_decimal_odds(odds_source)
  ```
- Replace betting math (lines 248-258) with:
  ```python
  bet = calculate_metrics(prob, decimal_odds, kelly_frac, max_per_race, bankroll)
  ```
- Replace qualifying check (lines 261-267) with:
  ```python
  qualifies = qualifies_for_bet(
      bet["ev"], prob, bet["overlay"], decimal_odds, bet["kelly"],
      min_ev=min_ev, min_prob=min_prob, min_overlay=min_overlay, max_odds=max_odds,
  )
  ```
- Replace feature preparation (lines 174-179) with:
  ```python
  X = prepare_feature_matrix(features_df, feature_columns)
  ```
- Replace outer try/except/traceback with `streamlit_error_boundary("Prediction")`

- [ ] **Step 2: Verify page compiles**

Run: `cd F:/racing && python -c "import py_compile; py_compile.compile('app/pages/5_Race_Predictions.py', doraise=True); print('OK')"`

- [ ] **Step 3: Verify no raw sqlite3 usage remains**

Run: `cd F:/racing && grep -n "sqlite3" app/pages/5_Race_Predictions.py` — expected: no output

- [ ] **Step 4: Commit**

```bash
git add app/pages/5_Race_Predictions.py
git commit -m "refactor: Predictions uses all shared utils (db, betting, features, model_selector)"
```

---

### Task 11: Refactor `backtesting/backtester.py`

**Files:**
- Modify: `backtesting/backtester.py`

- [ ] **Step 1: Replace `_to_decimal_odds` with shared utility**

Key changes:
- Add `from app.utils.betting import to_decimal_odds` at top of file (ensure `sys.path` includes project root)
- Replace call at line 299:
  ```python
  # Before:
  decimal_odds = self._to_decimal_odds(odds)
  # After:
  decimal_odds = to_decimal_odds(odds)
  ```
- Delete the `_to_decimal_odds` method (lines 363-373)

- [ ] **Step 2: Verify module compiles**

Run: `cd F:/racing && python -c "import py_compile; py_compile.compile('backtesting/backtester.py', doraise=True); print('OK')"`

- [ ] **Step 3: Verify no `_to_decimal_odds` remains**

Run: `cd F:/racing && grep -n "_to_decimal_odds" backtesting/backtester.py` — expected: no output

- [ ] **Step 4: Commit**

```bash
git add backtesting/backtester.py
git commit -m "refactor: backtester uses shared to_decimal_odds from app.utils.betting"
```

---

### Task 12: Refactor `app/components/sidebar.py`

**Files:**
- Modify: `app/components/sidebar.py`

- [ ] **Step 1: Replace inline metric display with shared component**

Key changes:
- Add `from app.components.metrics_display import display_model_metrics`
- Replace lines 68-80 (the inline metric columns) with:
  ```python
  display_model_metrics(metrics)
  ```
- Remove the `from app.components.tooltips import METRICS` import inside `render_sidebar()` (it's now handled by metrics_display)

- [ ] **Step 2: Verify module compiles**

Run: `cd F:/racing && python -c "import py_compile; py_compile.compile('app/components/sidebar.py', doraise=True); print('OK')"`

- [ ] **Step 3: Commit**

```bash
git add app/components/sidebar.py
git commit -m "refactor: sidebar uses shared metrics display component"
```

---

### Task 13: Final Verification

- [ ] **Step 1: Compile all files**

Run: `cd F:/racing && python -c "import py_compile; files = ['app/utils/__init__.py', 'app/utils/db.py', 'app/utils/betting.py', 'app/utils/features.py', 'app/components/model_selector.py', 'app/components/metrics_display.py', 'app/pages/1_Dashboard.py', 'app/pages/2_Data_Management.py', 'app/pages/3_Model_Training.py', 'app/pages/4_Backtesting.py', 'app/pages/5_Race_Predictions.py', 'app/pages/6_Settings.py', 'app/components/sidebar.py', 'backtesting/backtester.py']; [py_compile.compile(f, doraise=True) for f in files]; print('All OK')"`

- [ ] **Step 2: Verify no raw sqlite3 in pages**

Run: `cd F:/racing && grep -rn "sqlite3.connect" app/pages/` — expected: no output

- [ ] **Step 3: Verify no `_to_decimal_odds` remains**

Run: `cd F:/racing && grep -rn "_to_decimal_odds" backtesting/` — expected: no output

- [ ] **Step 4: Verify betting math consistency**

Run: `cd F:/racing && python -c "from app.utils.betting import to_decimal_odds; assert to_decimal_odds(5.0) == 6.0; assert to_decimal_odds(350) == 4.5; assert to_decimal_odds(-200) == 1.5; assert to_decimal_odds(None) == 2.0; assert to_decimal_odds('5/2') == 2.0; print('Betting math OK')"`

- [ ] **Step 5: Final commit (if any stragglers)**

```bash
git status
```
