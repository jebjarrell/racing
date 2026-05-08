# Claude Code Implementation Prompt: Pace Analysis + Speed Figure Adjustments

**Copy everything below the line into Claude Code.**

---

## Task

Implement two new feature categories for the horse racing prediction engine: **Pace Analysis Core** (8 new features) and **Speed Figure Adjustments** (4 new features). These are the highest-priority gaps in the current 43-feature model — pace and adjusted speed are the strongest predictive signals missing from the pipeline.

## Project Context

This is a LightGBM binary classifier that predicts win probability for thoroughbred horse races. The codebase uses SQLite for development, with a feature engine that calculates per-entry features using strict point-in-time logic (no data from on or after race_date may be used). Features are computed by sub-calculator classes instantiated in `FeatureEngine.__init__`, and results are merged in `calculate_entry_features()`. New features must also be registered in `models/feature_definitions.py`.

## Existing Code Pattern

Read these files to understand the architecture before writing any code:

- `features/feature_engine.py` — Main orchestrator. Study how `RollingStatsCalculator` and `TrackBiasCalculator` are initialized in `__init__`, how each category method (`calculate_horse_features`, `calculate_connection_features`, `calculate_track_features`) queries the DB and returns a `Dict[str, Any]`, and how `calculate_entry_features` merges all dicts. Also study `add_field_relative_features` for the ranking pattern.
- `features/rolling_stats.py` — Example sub-calculator. Note the `__init__(db_path, ...)` pattern, own `_get_connection()`, and `close()` method.
- `features/track_bias.py` — Another sub-calculator. Same pattern.
- `models/feature_definitions.py` — Contains `FEATURE_COLUMNS` list (43 features), `FEATURE_DEFINITIONS` dict with `FeatureDefinition` dataclass entries, and `FEATURE_SUMMARY` dict. All new features must be added here.
- `enhanced_schema.sql` — Database schema. The pace/position data tables are defined here.

## Database Tables Available for Pace Features

The data already exists in the database. You do NOT need to create new tables.

### `race_fractions` — Race-level fractional times
```sql
CREATE TABLE race_fractions (
    race_id VARCHAR(100),
    call_position INTEGER,     -- 1=first call, 2=second call, etc.
    distance_yards INTEGER,    -- distance at this call point
    fraction_time DECIMAL(8,3), -- cumulative time to this point
    leader_at_call VARCHAR(20), -- registration_number of leader
    PRIMARY KEY (race_id, call_position)
);
```

### `horse_position_calls` — Per-horse positions at each call
```sql
CREATE TABLE horse_position_calls (
    race_id VARCHAR(100),
    registration_number VARCHAR(20),
    call_position INTEGER,     -- 1=first call, 2=second call, etc.
    position INTEGER,          -- running position (1=first, 2=second...)
    lengths_behind DECIMAL(6,2), -- lengths behind leader
    PRIMARY KEY (race_id, registration_number, call_position)
);
```

### `race_entries_standardized` — Also has position columns
```sql
-- Relevant columns (among many others):
start_position INTEGER,
first_call_position INTEGER,
second_call_position INTEGER,
stretch_position INTEGER,
finish_position INTEGER,
speed_rating INTEGER,
final_time DECIMAL(8,3)
```

### `races_standardized` — Race-level timing
```sql
-- Relevant columns:
winning_time DECIMAL(8,3),
final_fraction_time DECIMAL(8,3),
distance_yards INTEGER,
track_code VARCHAR(10),
course_type_code VARCHAR(20),
track_condition VARCHAR(20)
```

## Feature Specifications

### Part 1: Pace Analysis Core (8 features)

Create a new file `features/pace_calculator.py` with a `PaceCalculator` class following the sub-calculator pattern.

**Features to implement:**

1. **`horse_pace_early`** (NUMERIC, default 0.0, range 0-10)
   - Average early-pace figure across horse's last 5 races (point-in-time).
   - Early pace = horse's position at first call, converted to a 0-10 scale where 1st = 10, 2nd = 9, etc. (capped at 0 for positions > 10).
   - Query `horse_position_calls` WHERE `call_position = 1` for the horse's past races.

2. **`horse_pace_mid`** (NUMERIC, default 0.0, range 0-10)
   - Same as above but for second call (`call_position = 2`).

3. **`horse_pace_late`** (NUMERIC, default 0.0, range -5 to 5)
   - Average late-pace gain/loss: `(second_call_position - finish_position)` from `race_entries_standardized` over last 5 races.
   - Positive = closer (gains ground late), negative = fades.

4. **`horse_pace_style`** (CATEGORICAL encoded as NUMERIC, default 2, range 1-4)
   - Classification based on early and late pace averages:
     - **1 = E (Early speed)**: `horse_pace_early >= 7` AND `horse_pace_late <= 0`
     - **2 = EP (Early/Presser)**: `horse_pace_early >= 5` AND `horse_pace_late > 0`
     - **3 = PS (Presser/Stalker)**: `horse_pace_early` between 3-5
     - **4 = S (Closer/Sustained)**: `horse_pace_early < 3` AND `horse_pace_late >= 2`
   - This is Brohamer's running style classification.

5. **`race_pace_scenario`** (NUMERIC, default 0.0, range -3 to 3)
   - Predicted pace scenario for the upcoming race based on the field composition.
   - Count entries with `horse_pace_style == 1` (E-types) in the current field.
   - Score: `(count_of_E_types - 1.5)` normalized by field_size. Positive = hot pace (multiple speed horses), negative = slow pace (no contested lead).
   - This requires computing pace_style for all entries in the race first, then deriving the field-level scenario.

6. **`horse_pace_fit_score`** (NUMERIC, default 0.0, range -3 to 3)
   - How well the horse's running style fits the predicted pace scenario.
   - Closers benefit from hot pace (positive fit), early speed benefits from slow pace (positive fit).
   - Formula: If style is E/EP → `pace_fit = -race_pace_scenario`; if style is PS/S → `pace_fit = race_pace_scenario`.

7. **`field_early_speed_count`** (NUMERIC, default 1, range 0-14)
   - Count of horses in the field with `horse_pace_style == 1` (E-type).
   - Field-level feature applied to all entries in the race.

8. **`horse_is_lone_speed`** (BINARY, default 0)
   - 1 if this horse is the ONLY E-type (`horse_pace_style == 1`) in the field.
   - Lone speed horses have a significant statistical edge.

### Part 2: Speed Figure Adjustments (4 features)

Add methods to the existing `TrackBiasCalculator` in `features/track_bias.py` OR create a new `features/speed_adjustments.py` — your choice based on cohesion.

**Features to implement:**

1. **`horse_speed_track_adjusted`** (NUMERIC, default 0.0, range 0-150)
   - Horse's best speed figure from last 90 days, adjusted by daily track variant.
   - Daily track variant = `(track_day_avg_speed - global_avg_speed)` for all races at that track on that day.
   - Adjustment: `raw_speed - track_variant_of_day_it_was_earned`.
   - Query `race_entries_standardized` for speed_ratings on the same track+date, compute the day's average, compare to a global average (compute from all races in the lookback window).

2. **`horse_speed_surface_adjusted`** (NUMERIC, default 0.0, range 0-150)
   - Speed figure adjusted for surface type (dirt vs turf).
   - If the horse's best speed was earned on a different surface than today's race, apply a conversion penalty.
   - Dirt-to-turf penalty: -3 points. Turf-to-dirt penalty: -2 points. Same surface: no adjustment.
   - Requires joining `race_entries_standardized` with `races_standardized` to get the surface of each past race.

3. **`horse_speed_class_adjusted`** (NUMERIC, default 0.0, range 0-150)
   - Speed figure adjusted for class level differential.
   - If the horse earned its best speed at a lower class than today's race, apply a penalty.
   - Penalty per class level up: -1.5 points. Bonus per class level down: +0.5 points.
   - Uses `races_standardized.class_level` for the race where speed was earned vs. current `race_context.class_level`.

4. **`daily_track_variant`** (NUMERIC, default 0.0, range -15 to 15)
   - The track variant for the current race's track on this race_date.
   - Use the same calculation as (1): average speed_rating on this track+date minus global average.
   - This is a race-level feature (same value for all entries in the race).
   - **Important**: For today's race, use the PREVIOUS day's variant as a proxy (you can't use today's results). If no previous day data, use the trailing 7-day average for that track.

## Implementation Steps

### Step 1: Create `features/pace_calculator.py`

```python
class PaceCalculator:
    """Calculate pace and running style features from position call data."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = None
    
    def _get_connection(self): ...
    def close(self): ...
    
    def calculate_horse_pace(self, registration_number: str, race_date: date, 
                              num_races: int = 5) -> Dict[str, Any]:
        """Return horse_pace_early, horse_pace_mid, horse_pace_late, horse_pace_style."""
        # Query horse_position_calls joined with races_standardized
        # WHERE race_date < target_date (POINT-IN-TIME)
        # ORDER BY race_date DESC LIMIT num_races
        ...
    
    def calculate_field_pace_features(self, race_id: str, race_date: date,
                                       entry_pace_styles: Dict[str, int]) -> Dict[str, Any]:
        """Return race_pace_scenario, field_early_speed_count.
        Called AFTER all individual pace styles are computed."""
        ...
    
    def calculate_pace_fit(self, horse_pace_style: int, 
                            race_pace_scenario: float) -> float:
        """Return horse_pace_fit_score."""
        ...
    
    def is_lone_speed(self, horse_pace_style: int, 
                       field_early_speed_count: int) -> bool:
        """Return horse_is_lone_speed."""
        ...
```

### Step 2: Create `features/speed_adjustments.py`

```python
class SpeedAdjustmentCalculator:
    """Calculate track-variant and surface/class adjusted speed figures."""
    
    def __init__(self, db_path: str, global_lookback_days: int = 365):
        self.db_path = db_path
        self.global_lookback_days = global_lookback_days
        self._conn = None
        self._global_avg_cache = {}  # Cache: (track, surface) -> avg_speed
    
    def _get_connection(self): ...
    def close(self): ...
    
    def calculate_daily_track_variant(self, track_code: str, race_date: date,
                                       course_type: str) -> float:
        """Track variant for previous day (or trailing 7-day avg)."""
        ...
    
    def calculate_adjusted_speeds(self, registration_number: str, race_date: date,
                                    race_context) -> Dict[str, Any]:
        """Return horse_speed_track_adjusted, horse_speed_surface_adjusted, 
        horse_speed_class_adjusted."""
        ...
```

### Step 3: Integrate into `features/feature_engine.py`

1. Import the new calculators at the top:
   ```python
   from .pace_calculator import PaceCalculator
   from .speed_adjustments import SpeedAdjustmentCalculator
   ```

2. Initialize them in `FeatureEngine.__init__`:
   ```python
   self.pace_calc = PaceCalculator(db_path=db_path)
   self.speed_adj = SpeedAdjustmentCalculator(db_path=db_path)
   ```

3. Close them in `FeatureEngine.close()`.

4. Add a new method `calculate_pace_features(self, entry, race_context)` that calls `self.pace_calc.calculate_horse_pace(...)` and returns the 4 horse-level pace features.

5. Add a new method `calculate_speed_adjustment_features(self, entry, race_context)` that calls `self.speed_adj`.

6. Call both new methods in `calculate_entry_features()`, merging results into the features dict.

7. **Critical — field-level pace features**: In `calculate_all_features()`, AFTER computing all individual entry features, do a second pass to compute the field-level pace features (`race_pace_scenario`, `horse_pace_fit_score`, `field_early_speed_count`, `horse_is_lone_speed`). This follows the same pattern as `add_field_relative_features()` — iterate over the features_list, compute field-level aggregates, then write them back. Create a new method `add_pace_field_features(self, features_list)` for this.

8. In `_process_race_chunk` (the parallel worker function), the new calculators are automatically available since it creates a fresh `FeatureEngine`.

### Step 4: Update `models/feature_definitions.py`

1. Add all 12 new features to the `FEATURE_COLUMNS` list under new comment sections:
   ```python
   # Pace features (8 features)
   'horse_pace_early', 'horse_pace_mid', 'horse_pace_late',
   'horse_pace_style', 'race_pace_scenario', 'horse_pace_fit_score',
   'field_early_speed_count', 'horse_is_lone_speed',
   
   # Speed adjustments (4 features)  
   'horse_speed_track_adjusted', 'horse_speed_surface_adjusted',
   'horse_speed_class_adjusted', 'daily_track_variant',
   ```

2. Add corresponding `FeatureDefinition` entries to the `FEATURE_DEFINITIONS` dict with proper types, defaults, ranges, and descriptions.

3. Update `FEATURE_SUMMARY` counts.

### Step 5: Write Tests

Create `tests/test_pace_calculator.py` and `tests/test_speed_adjustments.py`.

Test cases must cover:
- Point-in-time enforcement: verify no future data leaks into calculations
- Pace style classification boundaries (E/EP/PS/S edge cases)
- Lone speed detection with 0, 1, and 2+ E-types in field
- Track variant calculation with missing data (no previous day → 7-day fallback)
- Surface conversion penalties (dirt→turf, turf→dirt, same surface)
- Class adjustment with ascending and descending class changes
- Empty/insufficient history (horse with < 5 past races, horse with no position call data)
- Integration: verify all 12 new features appear in `calculate_all_features()` output

Look at existing test files in `tests/` for the project's test patterns and fixtures.

## Constraints

- **POINT-IN-TIME is non-negotiable.** Every query must filter `WHERE race_date < ?` using the target race date. Never use `<=` (the current race's data must not be included).
- **Handle missing data gracefully.** Many horses won't have position call data. Return the default values from `FeatureDefinition` when data is insufficient.
- **Minimum sample sizes.** If a horse has fewer than 3 past races with position calls, flag the pace features as low-confidence (but still compute from whatever is available).
- **SQLite compatibility.** The dev database is SQLite. Use standard SQL that works in both SQLite and PostgreSQL.
- **No new dependencies.** Only use stdlib + packages already in the project (numpy, pandas, scikit-learn).
- **Preserve existing behavior.** Do not modify any existing feature calculations. The 43 current features must remain unchanged.

## Validation

After implementation, run:
```bash
python -m pytest tests/ -v
```

Also verify the feature count:
```python
from models.feature_definitions import FEATURE_COLUMNS
assert len(FEATURE_COLUMNS) == 55  # 43 existing + 8 pace + 4 speed
```

And spot-check that `calculate_all_features()` returns dicts with all 55 feature keys populated.
