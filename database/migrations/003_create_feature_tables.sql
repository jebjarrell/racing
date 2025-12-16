-- =============================================================================
-- Migration 003: Create Feature Engineering Tables
-- Version: 1.0
-- Description: Creates tables for computed features and rolling statistics
-- =============================================================================

-- =============================================================================
-- ROLLING STATISTICS TABLES
-- =============================================================================

-- Trainer rolling statistics
CREATE TABLE IF NOT EXISTS features.trainer_rolling_stats (
    stat_id SERIAL PRIMARY KEY,
    trainer_id VARCHAR(20) NOT NULL,
    calculation_date DATE NOT NULL,
    window_days INTEGER NOT NULL,

    -- Performance metrics
    starts INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    places INTEGER DEFAULT 0,
    shows INTEGER DEFAULT 0,

    -- Rates
    win_rate DECIMAL(5,4),
    place_rate DECIMAL(5,4),
    show_rate DECIMAL(5,4),
    roi DECIMAL(8,4),

    -- Advanced metrics
    avg_finish_position DECIMAL(4,2),
    avg_odds DECIMAL(8,2),
    avg_morning_line DECIMAL(8,2),

    -- Context splits
    dirt_wins INTEGER DEFAULT 0,
    dirt_starts INTEGER DEFAULT 0,
    turf_wins INTEGER DEFAULT 0,
    turf_starts INTEGER DEFAULT 0,

    -- Sample size indicator
    sufficient_sample BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_trainer_date_window UNIQUE (trainer_id, calculation_date, window_days),
    CONSTRAINT fk_trainer_stats FOREIGN KEY (trainer_id) REFERENCES racing.trainers(external_party_id)
);

-- Create hypertable
SELECT create_hypertable('features.trainer_rolling_stats', 'calculation_date',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Jockey rolling statistics
CREATE TABLE IF NOT EXISTS features.jockey_rolling_stats (
    stat_id SERIAL PRIMARY KEY,
    jockey_id VARCHAR(20) NOT NULL,
    calculation_date DATE NOT NULL,
    window_days INTEGER NOT NULL,

    -- Performance metrics
    starts INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    places INTEGER DEFAULT 0,
    shows INTEGER DEFAULT 0,

    -- Rates
    win_rate DECIMAL(5,4),
    place_rate DECIMAL(5,4),
    show_rate DECIMAL(5,4),
    roi DECIMAL(8,4),

    -- Advanced metrics
    avg_finish_position DECIMAL(4,2),
    avg_odds DECIMAL(8,2),
    avg_morning_line DECIMAL(8,2),

    -- Context splits
    dirt_wins INTEGER DEFAULT 0,
    dirt_starts INTEGER DEFAULT 0,
    turf_wins INTEGER DEFAULT 0,
    turf_starts INTEGER DEFAULT 0,

    -- Sample size indicator
    sufficient_sample BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_jockey_date_window UNIQUE (jockey_id, calculation_date, window_days),
    CONSTRAINT fk_jockey_stats FOREIGN KEY (jockey_id) REFERENCES racing.jockeys(external_party_id)
);

-- Create hypertable
SELECT create_hypertable('features.jockey_rolling_stats', 'calculation_date',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Trainer-Jockey combination stats
CREATE TABLE IF NOT EXISTS features.trainer_jockey_combo_stats (
    stat_id SERIAL PRIMARY KEY,
    trainer_id VARCHAR(20) NOT NULL,
    jockey_id VARCHAR(20) NOT NULL,
    calculation_date DATE NOT NULL,
    window_days INTEGER NOT NULL,

    -- Performance metrics
    starts INTEGER DEFAULT 0,
    wins INTEGER DEFAULT 0,
    places INTEGER DEFAULT 0,
    shows INTEGER DEFAULT 0,

    -- Rates
    win_rate DECIMAL(5,4),
    roi DECIMAL(8,4),

    -- Comparison to individual rates
    synergy_score DECIMAL(6,4),

    -- Sample size indicator
    sufficient_sample BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_combo_date_window UNIQUE (trainer_id, jockey_id, calculation_date, window_days)
);

-- Track bias statistics
CREATE TABLE IF NOT EXISTS features.track_bias_stats (
    stat_id SERIAL PRIMARY KEY,
    track_code VARCHAR(10) NOT NULL,
    surface VARCHAR(20) NOT NULL,
    distance_bucket VARCHAR(20) NOT NULL,
    calculation_date DATE NOT NULL,
    window_days INTEGER NOT NULL,

    -- Post position win rates
    post_1_win_rate DECIMAL(5,4),
    post_2_win_rate DECIMAL(5,4),
    post_3_win_rate DECIMAL(5,4),
    post_4_win_rate DECIMAL(5,4),
    post_5_win_rate DECIMAL(5,4),
    post_6_win_rate DECIMAL(5,4),
    post_7_win_rate DECIMAL(5,4),
    post_8_win_rate DECIMAL(5,4),
    post_outside_win_rate DECIMAL(5,4),

    -- Overall bias metrics
    inside_bias_score DECIMAL(6,4),
    speed_bias_score DECIMAL(6,4),

    -- Sample size
    total_races INTEGER DEFAULT 0,
    sufficient_sample BOOLEAN DEFAULT FALSE,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_track_bias UNIQUE (track_code, surface, distance_bucket, calculation_date, window_days)
);

-- Horse rolling form
CREATE TABLE IF NOT EXISTS features.horse_rolling_form (
    stat_id SERIAL PRIMARY KEY,
    registration_number VARCHAR(20) NOT NULL,
    calculation_date DATE NOT NULL,

    -- Recent form
    days_since_last_race INTEGER,
    last_3_finishes VARCHAR(20),
    last_3_speed_avg DECIMAL(5,2),

    -- Career stats at surface
    dirt_starts INTEGER DEFAULT 0,
    dirt_wins INTEGER DEFAULT 0,
    dirt_avg_finish DECIMAL(4,2),
    turf_starts INTEGER DEFAULT 0,
    turf_wins INTEGER DEFAULT 0,
    turf_avg_finish DECIMAL(4,2),

    -- Distance preferences
    sprint_starts INTEGER DEFAULT 0,
    sprint_wins INTEGER DEFAULT 0,
    route_starts INTEGER DEFAULT 0,
    route_wins INTEGER DEFAULT 0,

    -- Speed figures
    best_speed_90_days INTEGER,
    avg_speed_90_days DECIMAL(5,2),
    speed_trend DECIMAL(5,2),

    -- Class metrics
    last_class_level INTEGER,
    avg_class_level DECIMAL(4,2),
    class_change INTEGER,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_horse_form UNIQUE (registration_number, calculation_date),
    CONSTRAINT fk_horse_form FOREIGN KEY (registration_number) REFERENCES racing.horses_master(registration_number)
);

-- =============================================================================
-- PRE-COMPUTED RACE FEATURES
-- =============================================================================

-- Pre-computed features for inference speed
CREATE TABLE IF NOT EXISTS features.race_features (
    feature_id SERIAL PRIMARY KEY,
    race_id VARCHAR(100) NOT NULL,
    entry_id VARCHAR(150) NOT NULL,

    -- Horse form features (20)
    days_since_last DECIMAL(6,2),
    last_finish_position INTEGER,
    last_3_avg_finish DECIMAL(4,2),
    last_speed_figure INTEGER,
    avg_speed_90_days DECIMAL(5,2),
    best_speed_90_days INTEGER,
    speed_trend DECIMAL(5,2),
    career_win_rate DECIMAL(5,4),
    surface_win_rate DECIMAL(5,4),
    distance_win_rate DECIMAL(5,4),
    class_change INTEGER,
    avg_class_level DECIMAL(4,2),
    total_starts INTEGER,
    total_wins INTEGER,
    layoff_indicator BOOLEAN,
    first_time_starter BOOLEAN,
    age_at_race INTEGER,
    weight_carried INTEGER,
    weight_change INTEGER,
    morning_line_odds DECIMAL(8,2),

    -- Connection features (20)
    trainer_win_rate_14d DECIMAL(5,4),
    trainer_win_rate_30d DECIMAL(5,4),
    trainer_win_rate_60d DECIMAL(5,4),
    trainer_roi_30d DECIMAL(8,4),
    jockey_win_rate_14d DECIMAL(5,4),
    jockey_win_rate_30d DECIMAL(5,4),
    jockey_win_rate_60d DECIMAL(5,4),
    jockey_roi_30d DECIMAL(8,4),
    combo_win_rate DECIMAL(5,4),
    combo_synergy_score DECIMAL(6,4),
    trainer_hot_streak BOOLEAN,
    jockey_hot_streak BOOLEAN,
    trainer_surface_win_rate DECIMAL(5,4),
    jockey_surface_win_rate DECIMAL(5,4),
    trainer_distance_win_rate DECIMAL(5,4),
    jockey_distance_win_rate DECIMAL(5,4),
    trainer_track_win_rate DECIMAL(5,4),
    jockey_track_win_rate DECIMAL(5,4),
    trainer_sample_flag BOOLEAN,
    jockey_sample_flag BOOLEAN,

    -- Speed/Pace features (10)
    speed_rank_in_field INTEGER,
    early_pace_figure DECIMAL(5,2),
    late_pace_figure DECIMAL(5,2),
    pace_style VARCHAR(20),
    projected_pace_scenario VARCHAR(20),
    speed_vs_field_avg DECIMAL(5,2),
    pace_vs_field_avg DECIMAL(5,2),
    track_variant_adjustment DECIMAL(5,2),
    speed_last_3_avg DECIMAL(5,2),
    pace_last_3_avg DECIMAL(5,2),

    -- Class features (10)
    class_rank_in_field INTEGER,
    purse_rank_in_field INTEGER,
    earnings_per_start DECIMAL(10,2),
    class_drop_indicator BOOLEAN,
    class_rise_indicator BOOLEAN,
    stakes_experience BOOLEAN,
    graded_stakes_experience BOOLEAN,
    last_purse DECIMAL(12,2),
    avg_purse DECIMAL(12,2),
    class_consistency DECIMAL(5,4),

    -- Track/Condition features (10)
    post_position INTEGER,
    post_position_win_rate DECIMAL(5,4),
    surface_preference DECIMAL(5,4),
    distance_preference DECIMAL(5,4),
    track_experience_starts INTEGER,
    track_experience_wins INTEGER,
    condition_preference DECIMAL(5,4),
    rail_bias_adjustment DECIMAL(5,4),
    field_size INTEGER,
    field_quality_score DECIMAL(5,2),

    -- Equipment features (10)
    blinkers_on BOOLEAN,
    blinkers_first_time BOOLEAN,
    blinkers_off BOOLEAN,
    lasix_on BOOLEAN,
    lasix_first_time BOOLEAN,
    equipment_change BOOLEAN,
    tongue_tie BOOLEAN,
    nasal_strip BOOLEAN,
    equipment_change_positive BOOLEAN,
    medication_change BOOLEAN,

    -- Meta features (5)
    trainer_sample_size_flag BOOLEAN,
    jockey_sample_size_flag BOOLEAN,
    combo_sample_size_flag BOOLEAN,
    horse_sample_size_flag BOOLEAN,
    track_bias_sample_flag BOOLEAN,

    created_at TIMESTAMPTZ DEFAULT NOW(),

    CONSTRAINT uq_race_entry_features UNIQUE (race_id, entry_id),
    CONSTRAINT fk_features_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id)
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_trainer_stats_date ON features.trainer_rolling_stats(calculation_date);
CREATE INDEX IF NOT EXISTS idx_trainer_stats_trainer ON features.trainer_rolling_stats(trainer_id);
CREATE INDEX IF NOT EXISTS idx_trainer_stats_window ON features.trainer_rolling_stats(trainer_id, window_days);

CREATE INDEX IF NOT EXISTS idx_jockey_stats_date ON features.jockey_rolling_stats(calculation_date);
CREATE INDEX IF NOT EXISTS idx_jockey_stats_jockey ON features.jockey_rolling_stats(jockey_id);
CREATE INDEX IF NOT EXISTS idx_jockey_stats_window ON features.jockey_rolling_stats(jockey_id, window_days);

CREATE INDEX IF NOT EXISTS idx_combo_stats_date ON features.trainer_jockey_combo_stats(calculation_date);
CREATE INDEX IF NOT EXISTS idx_combo_stats_trainer ON features.trainer_jockey_combo_stats(trainer_id);
CREATE INDEX IF NOT EXISTS idx_combo_stats_jockey ON features.trainer_jockey_combo_stats(jockey_id);

CREATE INDEX IF NOT EXISTS idx_track_bias_track ON features.track_bias_stats(track_code, surface, distance_bucket);
CREATE INDEX IF NOT EXISTS idx_track_bias_date ON features.track_bias_stats(calculation_date);

CREATE INDEX IF NOT EXISTS idx_horse_form_horse ON features.horse_rolling_form(registration_number);
CREATE INDEX IF NOT EXISTS idx_horse_form_date ON features.horse_rolling_form(calculation_date);

CREATE INDEX IF NOT EXISTS idx_race_features_race ON features.race_features(race_id);
CREATE INDEX IF NOT EXISTS idx_race_features_entry ON features.race_features(entry_id);

-- =============================================================================
-- MIGRATION TRACKING
-- =============================================================================

INSERT INTO public.schema_migrations (version, description)
VALUES ('003', 'Create feature engineering tables')
ON CONFLICT (version) DO NOTHING;
