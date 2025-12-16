-- =============================================================================
-- Migration 002: Create Core Racing Tables
-- Version: 1.0
-- Description: Creates core entity and race data tables
-- =============================================================================

-- =============================================================================
-- ENTITY TABLES
-- =============================================================================

-- Horse master data
CREATE TABLE IF NOT EXISTS racing.horses_master (
    registration_number VARCHAR(20) PRIMARY KEY,
    horse_name VARCHAR(200) NOT NULL,
    foaling_date DATE,
    year_of_birth INTEGER,
    sex_code VARCHAR(10),
    color_code VARCHAR(50),
    sire_registration VARCHAR(20),
    dam_registration VARCHAR(20),
    dam_sire_registration VARCHAR(20),
    breeder VARCHAR(200),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Trainers
CREATE TABLE IF NOT EXISTS racing.trainers (
    external_party_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    license_state VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Jockeys
CREATE TABLE IF NOT EXISTS racing.jockeys (
    external_party_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    license_state VARCHAR(50),
    weight_allowance INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Owners
CREATE TABLE IF NOT EXISTS racing.owners (
    external_party_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    ownership_type VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================================================
-- RACE DATA TABLES
-- =============================================================================

-- Races with standardized fields
CREATE TABLE IF NOT EXISTS racing.races (
    race_id VARCHAR(100) PRIMARY KEY,
    track_code VARCHAR(10) NOT NULL,
    race_date DATE NOT NULL,
    race_number INTEGER NOT NULL,

    -- Original race information
    race_name VARCHAR(500),
    conditions_text TEXT,

    -- Standardized categorical fields
    course_type_code VARCHAR(20),
    race_type_code VARCHAR(20),
    track_condition VARCHAR(20),

    -- Parsed restriction fields
    min_age INTEGER,
    max_age INTEGER,
    fillies_and_mares BOOLEAN DEFAULT FALSE,
    colts_and_geldings BOOLEAN DEFAULT FALSE,
    fillies_only BOOLEAN DEFAULT FALSE,
    mares_only BOOLEAN DEFAULT FALSE,
    colts_only BOOLEAN DEFAULT FALSE,
    geldings_only BOOLEAN DEFAULT FALSE,

    -- Standardized numeric fields
    distance_yards INTEGER,
    purse_usd DECIMAL(12,2),
    max_claim_price DECIMAL(12,2),
    min_claim_price DECIMAL(12,2),

    -- Race classification
    class_level INTEGER,
    purse_category VARCHAR(50),

    -- Timing and environmental
    post_time TIME,
    weather VARCHAR(100),
    wind_speed INTEGER,
    wind_direction VARCHAR(50),

    -- Race results
    field_size INTEGER,
    winning_time DECIMAL(8,3),
    winning_margin DECIMAL(6,2),
    final_fraction_time DECIMAL(8,3),

    -- Metadata
    source_file VARCHAR(500),
    data_source VARCHAR(50),
    extraction_date TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Constraints
    CONSTRAINT fk_race_course_type FOREIGN KEY (course_type_code) REFERENCES racing.course_types(code),
    CONSTRAINT fk_race_race_type FOREIGN KEY (race_type_code) REFERENCES racing.race_types(code),
    CONSTRAINT fk_race_track_condition FOREIGN KEY (track_condition) REFERENCES racing.track_conditions(code)
);

-- Convert to hypertable for time-series optimization
SELECT create_hypertable('racing.races', 'race_date',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE,
    migrate_data => TRUE
);

-- Race entries
CREATE TABLE IF NOT EXISTS racing.race_entries (
    entry_id VARCHAR(150) PRIMARY KEY,
    race_id VARCHAR(100) NOT NULL,
    registration_number VARCHAR(20) NOT NULL,

    -- Basic entry information
    program_number VARCHAR(10),
    post_position INTEGER,

    -- Standardized physical data
    weight_lbs INTEGER,
    age_at_race INTEGER,

    -- Equipment and medication flags
    has_blinkers BOOLEAN DEFAULT FALSE,
    has_lasix BOOLEAN DEFAULT FALSE,
    has_tongue_tie BOOLEAN DEFAULT FALSE,
    has_nasal_strip BOOLEAN DEFAULT FALSE,
    has_shadow_roll BOOLEAN DEFAULT FALSE,
    has_cheek_pieces BOOLEAN DEFAULT FALSE,
    has_ear_plugs BOOLEAN DEFAULT FALSE,
    has_hood BOOLEAN DEFAULT FALSE,

    -- Equipment change indicators
    equipment_change_indicator VARCHAR(50),
    lasix_first_time BOOLEAN DEFAULT FALSE,
    blinkers_first_time BOOLEAN DEFAULT FALSE,
    blinkers_off BOOLEAN DEFAULT FALSE,

    -- Claiming and wagering
    claim_price DECIMAL(10,2),
    morning_line_odds DECIMAL(8,2),

    -- Performance data
    official_finish_position INTEGER,
    actual_odds DECIMAL(8,2),
    win_payoff DECIMAL(8,2),
    place_payoff DECIMAL(8,2),
    show_payoff DECIMAL(8,2),

    -- Speed and time data
    final_time DECIMAL(8,3),
    speed_rating INTEGER,

    -- Trip and pace information
    start_position INTEGER,
    first_call_position INTEGER,
    second_call_position INTEGER,
    stretch_position INTEGER,
    finish_position INTEGER,
    beaten_lengths DECIMAL(6,2),

    -- Connections
    trainer_id VARCHAR(20),
    jockey_id VARCHAR(20),
    owner_id VARCHAR(20),

    -- Comments and notes
    race_comments TEXT,
    scratched BOOLEAN DEFAULT FALSE,
    scratch_reason VARCHAR(200),

    -- Metadata
    source_file VARCHAR(500),
    data_source VARCHAR(50),
    extraction_date TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),

    -- Constraints
    CONSTRAINT fk_entry_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id),
    CONSTRAINT fk_entry_horse FOREIGN KEY (registration_number) REFERENCES racing.horses_master(registration_number),
    CONSTRAINT fk_entry_trainer FOREIGN KEY (trainer_id) REFERENCES racing.trainers(external_party_id),
    CONSTRAINT fk_entry_jockey FOREIGN KEY (jockey_id) REFERENCES racing.jockeys(external_party_id),
    CONSTRAINT fk_entry_owner FOREIGN KEY (owner_id) REFERENCES racing.owners(external_party_id),
    CONSTRAINT uq_race_horse UNIQUE (race_id, registration_number)
);

-- =============================================================================
-- SUPPORTING TABLES
-- =============================================================================

-- Equipment details
CREATE TABLE IF NOT EXISTS racing.horse_race_equipment (
    race_id VARCHAR(100),
    registration_number VARCHAR(20),
    equipment_code VARCHAR(20),
    equipment_description VARCHAR(100),
    is_first_time BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (race_id, registration_number, equipment_code),
    CONSTRAINT fk_equip_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id),
    CONSTRAINT fk_equip_horse FOREIGN KEY (registration_number) REFERENCES racing.horses_master(registration_number),
    CONSTRAINT fk_equip_type FOREIGN KEY (equipment_code) REFERENCES racing.equipment_types(code)
);

-- Race fractional times
CREATE TABLE IF NOT EXISTS racing.race_fractions (
    race_id VARCHAR(100),
    call_position INTEGER,
    distance_yards INTEGER,
    fraction_time DECIMAL(8,3),
    leader_at_call VARCHAR(20),
    PRIMARY KEY (race_id, call_position),
    CONSTRAINT fk_frac_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id),
    CONSTRAINT fk_frac_leader FOREIGN KEY (leader_at_call) REFERENCES racing.horses_master(registration_number)
);

-- Horse position calls
CREATE TABLE IF NOT EXISTS racing.horse_position_calls (
    race_id VARCHAR(100),
    registration_number VARCHAR(20),
    call_position INTEGER,
    position INTEGER,
    lengths_behind DECIMAL(6,2),
    PRIMARY KEY (race_id, registration_number, call_position),
    CONSTRAINT fk_pos_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id),
    CONSTRAINT fk_pos_horse FOREIGN KEY (registration_number) REFERENCES racing.horses_master(registration_number)
);

-- Race wagering pools
CREATE TABLE IF NOT EXISTS racing.race_wagering (
    race_id VARCHAR(100),
    wager_type VARCHAR(50),
    pool_total DECIMAL(12,2),
    winning_combinations TEXT,
    payout DECIMAL(10,2),
    number_of_winners INTEGER,
    PRIMARY KEY (race_id, wager_type),
    CONSTRAINT fk_wager_race FOREIGN KEY (race_id) REFERENCES racing.races(race_id)
);

-- =============================================================================
-- INDEXES
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_races_date ON racing.races(race_date);
CREATE INDEX IF NOT EXISTS idx_races_track_date ON racing.races(track_code, race_date);
CREATE INDEX IF NOT EXISTS idx_races_type ON racing.races(race_type_code);
CREATE INDEX IF NOT EXISTS idx_races_class ON racing.races(class_level);

CREATE INDEX IF NOT EXISTS idx_entries_race ON racing.race_entries(race_id);
CREATE INDEX IF NOT EXISTS idx_entries_horse ON racing.race_entries(registration_number);
CREATE INDEX IF NOT EXISTS idx_entries_trainer ON racing.race_entries(trainer_id);
CREATE INDEX IF NOT EXISTS idx_entries_jockey ON racing.race_entries(jockey_id);
CREATE INDEX IF NOT EXISTS idx_entries_finish ON racing.race_entries(official_finish_position);

CREATE INDEX IF NOT EXISTS idx_horses_name ON racing.horses_master(horse_name);
CREATE INDEX IF NOT EXISTS idx_trainers_name ON racing.trainers(last_name, first_name);
CREATE INDEX IF NOT EXISTS idx_jockeys_name ON racing.jockeys(last_name, first_name);

-- =============================================================================
-- VIEWS
-- =============================================================================

-- Complete race entry view
CREATE OR REPLACE VIEW racing.vw_race_entries_complete AS
SELECT
    re.*,
    r.race_date,
    r.track_code,
    r.race_number,
    r.course_type_code,
    r.race_type_code,
    r.track_condition,
    r.distance_yards,
    r.purse_usd,
    r.class_level,
    r.purse_category,
    r.field_size,
    h.horse_name,
    h.foaling_date,
    h.year_of_birth,
    h.sex_code,
    h.color_code,
    t.first_name AS trainer_first_name,
    t.last_name AS trainer_last_name,
    j.first_name AS jockey_first_name,
    j.last_name AS jockey_last_name,
    o.first_name AS owner_first_name,
    o.last_name AS owner_last_name
FROM racing.race_entries re
JOIN racing.races r ON re.race_id = r.race_id
JOIN racing.horses_master h ON re.registration_number = h.registration_number
LEFT JOIN racing.trainers t ON re.trainer_id = t.external_party_id
LEFT JOIN racing.jockeys j ON re.jockey_id = j.external_party_id
LEFT JOIN racing.owners o ON re.owner_id = o.external_party_id;

-- =============================================================================
-- MIGRATION TRACKING
-- =============================================================================

INSERT INTO public.schema_migrations (version, description)
VALUES ('002', 'Create core racing tables')
ON CONFLICT (version) DO NOTHING;
