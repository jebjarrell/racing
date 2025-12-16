# Horse Racing Feature Catalog

**Version:** 1.0
**Last Updated:** 2025-12-16
**Total Features:** 115

---

## Table of Contents

1. [Overview](#1-overview)
2. [Feature Categories](#2-feature-categories)
3. [Horse Form Features (20)](#3-horse-form-features)
4. [Connection Features (20)](#4-connection-features)
5. [Speed & Pace Features (25)](#5-speed--pace-features)
6. [Class Features (15)](#6-class-features)
7. [Track & Condition Features (15)](#7-track--condition-features)
8. [Equipment Features (10)](#8-equipment-features)
9. [Meta Features (10)](#9-meta-features)
10. [Point-in-Time Validation](#10-point-in-time-validation)
11. [Feature Importance Baseline](#11-feature-importance-baseline)

---

## 1. Overview

This document catalogs all predictive features used in the win probability model. Each feature is computed with strict **point-in-time** integrity, meaning only data available before the race is used.

### 1.1 Feature Naming Convention

```
{category}_{metric}_{window/modifier}
```

Examples:
- `horse_speed_avg_3` - Horse's average speed figure over last 3 races
- `trainer_win_pct_30d` - Trainer's win percentage over last 30 days
- `field_speed_rank` - Horse's speed rank within the current field

### 1.2 Data Types

| Type | Description | Example |
|------|-------------|---------|
| `float` | Continuous numeric value | 0.0 - 1.0 for rates |
| `int` | Integer value | 1-14 for post position |
| `bool` | Binary flag | True/False for equipment |
| `ordinal` | Ordered category | 1-10 for class level |

---

## 2. Feature Categories

| Category | Count | Description |
|----------|-------|-------------|
| Horse Form | 20 | Recent performance and consistency |
| Connections | 20 | Trainer, jockey, and combo stats |
| Speed & Pace | 25 | Speed figures and pace analysis |
| Class | 15 | Race class and competitive level |
| Track & Conditions | 15 | Surface, distance, post position bias |
| Equipment | 10 | Equipment and medication indicators |
| Meta | 10 | Sample size and confidence flags |
| **Total** | **115** | |

---

## 3. Horse Form Features

### 3.1 Recent Performance

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `horse_days_since_last` | int | `race_date - last_race_date` | `race_entries_standardized` |
| `horse_days_since_2nd_last` | int | `race_date - 2nd_last_race_date` | `race_entries_standardized` |
| `horse_layoff_indicator` | bool | `days_since_last > 60` | Computed |
| `horse_recent_form_1_2_3` | int | Count of top-3 finishes in last 5 | `race_entries_standardized` |
| `horse_recent_form_wins` | int | Count of wins in last 5 | `race_entries_standardized` |

### 3.2 Career Statistics

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `horse_career_starts` | int | Total career starts | Aggregated |
| `horse_career_wins` | int | Total career wins | Aggregated |
| `horse_career_win_pct` | float | `wins / starts` | Computed |
| `horse_career_itm_pct` | float | `(1st + 2nd + 3rd) / starts` | Computed |
| `horse_career_roi` | float | `(total_payoffs - total_bet) / total_bet` | Computed |

### 3.3 Consistency Metrics

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `horse_finish_pos_avg_5` | float | Average finish position last 5 | Computed |
| `horse_finish_pos_std_5` | float | Std dev of finish position last 5 | Computed |
| `horse_beaten_lengths_avg` | float | Average beaten lengths last 5 | `race_entries_standardized` |
| `horse_improvement_trend` | float | Slope of finish positions last 5 | Computed |

### 3.4 Recency Adjustments

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `horse_is_first_start` | bool | No prior races | Computed |
| `horse_is_second_start` | bool | Exactly one prior race | Computed |
| `horse_returning_from_layoff` | bool | `days_since_last > 90` | Computed |
| `horse_back_to_back` | bool | `days_since_last < 14` | Computed |
| `horse_optimal_rest` | bool | `14 <= days_since_last <= 45` | Computed |

**Point-in-Time Validation:**
All horse form features computed using only races with `race_date < current_race_date`.

---

## 4. Connection Features

### 4.1 Trainer Rolling Statistics

| Feature Name | Type | Window | Formula |
|--------------|------|--------|---------|
| `trainer_starts_14d` | int | 14 days | Count of starts |
| `trainer_wins_14d` | int | 14 days | Count of wins |
| `trainer_win_pct_14d` | float | 14 days | `wins / starts` |
| `trainer_win_pct_30d` | float | 30 days | `wins / starts` |
| `trainer_win_pct_60d` | float | 60 days | `wins / starts` |
| `trainer_roi_30d` | float | 30 days | `(payoffs - bets) / bets` |

### 4.2 Jockey Rolling Statistics

| Feature Name | Type | Window | Formula |
|--------------|------|--------|---------|
| `jockey_starts_14d` | int | 14 days | Count of starts |
| `jockey_wins_14d` | int | 14 days | Count of wins |
| `jockey_win_pct_14d` | float | 14 days | `wins / starts` |
| `jockey_win_pct_30d` | float | 30 days | `wins / starts` |
| `jockey_win_pct_60d` | float | 60 days | `wins / starts` |
| `jockey_roi_30d` | float | 30 days | `(payoffs - bets) / bets` |

### 4.3 Trainer-Jockey Combinations

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `combo_starts_all` | int | All-time combo starts | Lifetime |
| `combo_wins_all` | int | All-time combo wins | Lifetime |
| `combo_win_pct_all` | float | All-time combo win rate | Lifetime |
| `combo_starts_1yr` | int | Last 365 days starts | Rolling |
| `combo_is_first_time` | bool | No prior combo races | Flag |

### 4.4 Specialization

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `trainer_surface_win_pct` | float | Win rate on today's surface | Dirt/Turf/Synth |
| `jockey_surface_win_pct` | float | Win rate on today's surface | Dirt/Turf/Synth |
| `trainer_distance_win_pct` | float | Win rate at distance bucket | Sprint/Route |
| `jockey_track_win_pct` | float | Win rate at this track | Track-specific |

**Point-in-Time Validation:**
All trainer/jockey stats computed using races where `race_date < current_race_date`.

---

## 5. Speed & Pace Features

### 5.1 Speed Figures (from Equibase)

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `horse_speed_last` | int | Most recent SpeedFigure | `race_entries_standardized.speed_rating` |
| `horse_speed_best_3` | int | Best SpeedFigure in last 3 | Computed |
| `horse_speed_avg_3` | float | Average SpeedFigure last 3 | Computed |
| `horse_speed_avg_5` | float | Average SpeedFigure last 5 | Computed |
| `horse_speed_std_5` | float | Std dev of SpeedFigure last 5 | Computed |
| `horse_speed_trend` | float | Slope of last 5 figures | Linear regression |

### 5.2 Pace Figures

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `horse_pace_early_avg` | float | Average PaceFigure1 last 3 | Equibase PaceFigure1 |
| `horse_pace_mid_avg` | float | Average PaceFigure2 last 3 | Equibase PaceFigure2 |
| `horse_pace_late_avg` | float | Average PaceFigure3 last 3 | Equibase PaceFigure3 |
| `horse_pace_style` | ordinal | 1=Front, 2=Presser, 3=Closer | Computed from positions |

### 5.3 Relative Speed Features (vs Field)

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `field_speed_rank` | int | Rank by best recent speed | 1 = best |
| `field_speed_vs_median` | float | `horse_speed - field_median` | Positive = above median |
| `field_speed_percentile` | float | Percentile in field | 0-1 |
| `field_pace_early_rank` | int | Rank by early pace | 1 = fastest early |

### 5.4 Pace Shape Compatibility

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `race_pace_scenario` | ordinal | Expected pace shape | 1=Slow, 2=Avg, 3=Fast |
| `horse_pace_fit_score` | float | How well running style fits scenario | -1 to +1 |
| `field_early_speed_count` | int | Number of early speed horses | Count |
| `horse_is_lone_speed` | bool | Only early speed horse | Flag |

### 5.5 Speed Adjustments

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `horse_speed_track_adjusted` | float | Speed adjusted for track variant | Computed |
| `horse_speed_surface_adjusted` | float | Speed adjusted for surface | Dirt/Turf differential |
| `horse_speed_class_adjusted` | float | Speed adjusted for class level | Normalized |

**Point-in-Time Validation:**
All speed features computed using only pre-race data. Track variants computed from historical data only.

---

## 6. Class Features

### 6.1 Class Level

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `race_class_level` | ordinal | 1-10 class hierarchy | `races_standardized.class_level` |
| `horse_last_class_level` | ordinal | Class level of last race | Historical |
| `horse_avg_class_level_3` | float | Avg class level last 3 | Computed |

### 6.2 Class Movement

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `horse_class_drop` | bool | Today's class < last class | Flag |
| `horse_class_rise` | bool | Today's class > last class | Flag |
| `horse_class_delta` | int | `today_class - last_class` | Signed integer |
| `horse_class_drop_amount` | int | `last_class - today_class` if drop | Non-negative |

### 6.3 Class Performance

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `horse_win_pct_at_class` | float | Win rate at today's class level | Historical |
| `horse_itm_pct_at_class` | float | ITM rate at today's class level | Historical |
| `horse_best_class_won` | ordinal | Highest class level won | Historical |
| `horse_class_ceiling` | ordinal | Highest class level run | Historical |

### 6.4 Relative Class Features

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `field_class_rank` | int | Rank by class experience | 1 = most experienced |
| `field_class_vs_median` | float | Class experience vs median | Positive = above |
| `horse_class_overqualified` | bool | Won at higher class | Flag |

**Point-in-Time Validation:**
All class features use historical race data with `race_date < current_race_date`.

---

## 7. Track & Condition Features

### 7.1 Surface Preference

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `horse_surface_starts` | int | Starts on today's surface | Historical |
| `horse_surface_wins` | int | Wins on today's surface | Historical |
| `horse_surface_win_pct` | float | Win rate on surface | Computed |
| `horse_surface_itm_pct` | float | ITM rate on surface | Computed |
| `horse_is_surface_debut` | bool | First time on surface | Flag |

### 7.2 Distance Preference

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `horse_distance_starts` | int | Starts at similar distance (±1f) | Historical |
| `horse_distance_wins` | int | Wins at similar distance | Historical |
| `horse_distance_win_pct` | float | Win rate at distance | Computed |
| `horse_is_distance_debut` | bool | First time at distance | Flag |
| `horse_stretch_out` | bool | Longer than last race | Flag |
| `horse_cut_back` | bool | Shorter than last race | Flag |

### 7.3 Post Position Bias

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `horse_post_position` | int | Raw post position | `race_entries_standardized` |
| `post_bias_win_pct` | float | Historical win rate for post | `features.track_bias_stats` |
| `post_bias_deviation` | float | `post_win_pct - (1/field_size)` | Computed |
| `post_bias_advantage` | float | Normalized bias score | Z-score |

**Bias Calculation (track×surface×distance_bucket):**
```sql
SELECT
    track_code,
    surface,
    distance_bucket,
    post_position,
    COUNT(*) as starts,
    SUM(CASE WHEN finish = 1 THEN 1 ELSE 0 END) as wins,
    wins / starts as win_pct
FROM race_entries
WHERE race_date < :current_date
GROUP BY track_code, surface, distance_bucket, post_position
HAVING starts >= 50;  -- Minimum sample size
```

### 7.4 Track-Specific Performance

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `horse_track_starts` | int | Starts at this track | Historical |
| `horse_track_wins` | int | Wins at this track | Historical |
| `horse_track_win_pct` | float | Win rate at track | Computed |
| `horse_is_track_debut` | bool | First time at track | Flag |

### 7.5 Condition Preferences

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `track_condition` | ordinal | 1=Fast, 2=Good, 3=Off | Encoded |
| `horse_off_track_win_pct` | float | Win rate on off tracks | Muddy/Sloppy |
| `horse_prefers_off_track` | bool | Better on off tracks | Computed |

**Point-in-Time Validation:**
Track bias calculated using only historical data. No same-day race data used.

---

## 8. Equipment Features

### 8.1 Current Equipment

| Feature Name | Type | Formula | Data Source |
|--------------|------|---------|-------------|
| `has_blinkers` | bool | Wearing blinkers | `race_entries_standardized` |
| `has_lasix` | bool | On Lasix/Salix | `race_entries_standardized` |
| `has_tongue_tie` | bool | Wearing tongue tie | `race_entries_standardized` |
| `has_nasal_strip` | bool | Wearing nasal strip | `race_entries_standardized` |
| `has_shadow_roll` | bool | Wearing shadow roll | `race_entries_standardized` |

### 8.2 Equipment Changes

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `blinkers_first_time` | bool | First time in blinkers | Historical comparison |
| `blinkers_off` | bool | Removing blinkers | Historical comparison |
| `lasix_first_time` | bool | First time on Lasix | Historical comparison |
| `equipment_change_any` | bool | Any equipment change | Computed |

### 8.3 Equipment Success Rates

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `trainer_blinkers_1st_win_pct` | float | Trainer's win rate with 1st time blinkers | Historical |
| `overall_blinkers_1st_impact` | float | General impact of 1st time blinkers | Population-level |

**Point-in-Time Validation:**
Equipment change detection requires comparing to previous race equipment.

---

## 9. Meta Features

### 9.1 Sample Size Indicators

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `horse_sample_size` | int | Number of prior races | Count |
| `trainer_sample_size` | int | Trainer starts in window | Count |
| `jockey_sample_size` | int | Jockey starts in window | Count |
| `combo_sample_size` | int | Combo starts all-time | Count |
| `post_bias_sample_size` | int | Races for bias calculation | Count |

### 9.2 Low Sample Flags

| Feature Name | Type | Formula | Threshold |
|--------------|------|---------|-----------|
| `horse_low_sample` | bool | `horse_sample_size < 3` | Flag |
| `trainer_low_sample` | bool | `trainer_sample_size < 20` | Flag |
| `jockey_low_sample` | bool | `jockey_sample_size < 20` | Flag |
| `combo_low_sample` | bool | `combo_sample_size < 5` | Flag |
| `bias_low_sample` | bool | `post_bias_sample_size < 50` | Flag |

### 9.3 Data Quality Flags

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `missing_speed_figure` | bool | No speed figure available | Quality flag |
| `missing_pace_figures` | bool | No pace figures available | Quality flag |
| `incomplete_pp_data` | bool | Partial past performances | Quality flag |
| `data_recency_days` | int | Days since last data point | Freshness |

### 9.4 Confidence Scores

| Feature Name | Type | Formula | Notes |
|--------------|------|---------|-------|
| `feature_confidence` | float | Overall feature reliability | 0-1 scale |
| `prediction_confidence` | float | Model confidence in prediction | 0-1 scale |

**Usage:**
Meta features help the model weight predictions appropriately when sample sizes are low.

---

## 10. Point-in-Time Validation

### 10.1 Validation Framework

Every feature must pass the following validation:

```python
def validate_no_leakage(race_id: str, features: Dict) -> bool:
    """
    Recompute features excluding target race and verify identical values.
    """
    # Get the race date
    race_date = get_race_date(race_id)

    # Compute features including the race
    features_with = compute_features(race_id, include_race=True)

    # Compute features excluding the race
    features_without = compute_features(race_id, include_race=False)

    # Features should be identical (race result not used)
    return features_with == features_without
```

### 10.2 Common Leakage Patterns to Avoid

| Pattern | Example | How to Avoid |
|---------|---------|--------------|
| Using final odds | Training on final odds | Only use morning line as feature |
| Future race info | Using next race result | Strict date filtering |
| Same-day bias | Using earlier races same day | Use previous day cutoff |
| Rolling window error | Including target in window | Exclude target race explicitly |

### 10.3 Validation Procedure

1. Before each model training, run validation on 100+ random races
2. Document all validation runs in `monitoring.validation_runs`
3. If any validation fails, investigate and fix before proceeding

---

## 11. Feature Importance Baseline

### 11.1 Expected High-Importance Features

Based on domain knowledge and prior research:

| Feature | Expected Importance | Rationale |
|---------|---------------------|-----------|
| `horse_speed_avg_3` | Very High | Core predictive signal |
| `field_speed_rank` | Very High | Relative strength |
| `horse_class_drop` | High | Well-known handicapping angle |
| `trainer_win_pct_30d` | High | Recent form matters |
| `jockey_win_pct_30d` | High | Recent form matters |
| `horse_days_since_last` | Medium | Freshness factor |
| `post_bias_deviation` | Medium | Track-specific edge |
| `blinkers_first_time` | Medium | Equipment changes impactful |

### 11.2 Expected Low-Importance Features

| Feature | Expected Importance | Notes |
|---------|---------------------|-------|
| `horse_career_starts` | Low | Less predictive than recent |
| `horse_color` | None | Not used |
| `owner_id` | None | Not predictive |

### 11.3 Red Flags

If these features show high importance, investigate for leakage:

| Feature | Red Flag Threshold |
|---------|-------------------|
| `final_odds` | Any importance (should not be a feature) |
| `actual_finish` | Any importance (should not be a feature) |
| `winning_time` | Any importance (should not be a feature) |

---

## Appendix A: Feature Implementation Status

| Category | Count | Implemented | Validated |
|----------|-------|-------------|-----------|
| Horse Form | 20 | [ ] | [ ] |
| Connections | 20 | [ ] | [ ] |
| Speed & Pace | 25 | [ ] | [ ] |
| Class | 15 | [ ] | [ ] |
| Track & Conditions | 15 | [ ] | [ ] |
| Equipment | 10 | [ ] | [ ] |
| Meta | 10 | [ ] | [ ] |

---

## Appendix B: SQL Queries for Feature Computation

### Rolling Trainer Stats

```sql
WITH trainer_races AS (
    SELECT
        trainer_id,
        race_date,
        official_finish_position,
        win_payoff
    FROM vw_race_entries_complete
    WHERE race_date < :target_date
      AND race_date >= :target_date - INTERVAL ':window_days days'
)
SELECT
    trainer_id,
    COUNT(*) as starts,
    SUM(CASE WHEN official_finish_position = 1 THEN 1 ELSE 0 END) as wins,
    wins::float / NULLIF(starts, 0) as win_pct,
    (SUM(win_payoff) - COUNT(*) * 2) / (COUNT(*) * 2) as roi
FROM trainer_races
GROUP BY trainer_id;
```

### Post Position Bias

```sql
SELECT
    track_code,
    course_type_code as surface,
    CASE
        WHEN distance_yards < 1540 THEN 'sprint'  -- < 7f
        WHEN distance_yards < 2200 THEN 'route'   -- 7f to < 1m
        ELSE 'marathon'
    END as distance_bucket,
    post_position,
    COUNT(*) as starts,
    SUM(CASE WHEN official_finish_position = 1 THEN 1 ELSE 0 END) as wins,
    wins::float / starts as win_pct
FROM vw_race_entries_complete
WHERE race_date < :target_date
  AND official_finish_position IS NOT NULL
GROUP BY track_code, surface, distance_bucket, post_position
HAVING COUNT(*) >= 50;
```

---

*Document maintained by: Data Science Team*
*Review cycle: Before each model training*
