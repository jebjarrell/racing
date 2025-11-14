-- Enhanced Racing Pipeline Database Schema
-- Includes standardization tables and normalized race/entry data
-- Designed for machine learning feature engineering

-- ============================================================================
-- REFERENCE/LOOKUP TABLES FOR STANDARDIZATION
-- ============================================================================

-- Course types and surface categories
CREATE TABLE IF NOT EXISTS course_types (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(100),
    surface_category VARCHAR(20), -- 'dirt', 'turf', 'synthetic'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Race type classifications with hierarchy
CREATE TABLE IF NOT EXISTS race_types (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(200),
    class_level INTEGER NOT NULL, -- 1=lowest (maiden), 10=highest (G1)
    purse_category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Equipment standardization
CREATE TABLE IF NOT EXISTS equipment_types (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(100),
    equipment_category VARCHAR(50), -- 'vision', 'respiratory', 'medication', etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Track condition mappings
CREATE TABLE IF NOT EXISTS track_conditions (
    code VARCHAR(20) PRIMARY KEY,
    description VARCHAR(100),
    surface_speed VARCHAR(20), -- 'fast', 'slow', 'average'
    bias_tendency VARCHAR(50), -- 'speed', 'closer', 'neutral'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- STANDARDIZED RACE DATA TABLES  
-- ============================================================================

-- Enhanced races table with standardized fields
CREATE TABLE IF NOT EXISTS races_standardized (
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
    
    -- Race results (when available)
    winning_time DECIMAL(8,3),
    winning_margin DECIMAL(6,2),
    final_fraction_time DECIMAL(8,3),
    
    -- Metadata
    source_file VARCHAR(500),
    data_source VARCHAR(50), -- 'past_performance' or 'result_chart'
    extraction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (course_type_code) REFERENCES course_types(code),
    FOREIGN KEY (race_type_code) REFERENCES race_types(code),
    FOREIGN KEY (track_condition) REFERENCES track_conditions(code)
);

-- ============================================================================
-- HORSE ENTRY DATA WITH STANDARDIZED FEATURES
-- ============================================================================

-- Race entries with standardized horse data
CREATE TABLE IF NOT EXISTS race_entries_standardized (
    entry_id VARCHAR(150) PRIMARY KEY, -- race_id + registration_number
    race_id VARCHAR(100) NOT NULL,
    registration_number VARCHAR(20) NOT NULL,
    
    -- Basic entry information
    program_number VARCHAR(10),
    post_position INTEGER,
    
    -- Standardized physical data
    weight_lbs INTEGER,
    age_at_race INTEGER,
    
    -- Equipment and medication (boolean flags for common items)
    has_blinkers BOOLEAN DEFAULT FALSE,
    has_lasix BOOLEAN DEFAULT FALSE,
    has_tongue_tie BOOLEAN DEFAULT FALSE,
    has_nasal_strip BOOLEAN DEFAULT FALSE,
    has_shadow_roll BOOLEAN DEFAULT FALSE,
    has_cheek_pieces BOOLEAN DEFAULT FALSE,
    has_ear_plugs BOOLEAN DEFAULT FALSE,
    has_hood BOOLEAN DEFAULT FALSE,
    
    -- Equipment/medication change indicators
    equipment_change_indicator VARCHAR(50), -- 'first_time', 'off', 'on'
    lasix_first_time BOOLEAN DEFAULT FALSE,
    blinkers_first_time BOOLEAN DEFAULT FALSE,
    blinkers_off BOOLEAN DEFAULT FALSE,
    
    -- Claiming and wagering
    claim_price DECIMAL(10,2),
    morning_line_odds DECIMAL(8,2),
    
    -- Performance data (when from results)
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
    data_source VARCHAR(50), -- 'past_performance' or 'result_chart'
    extraction_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign key constraints
    FOREIGN KEY (race_id) REFERENCES races_standardized(race_id),
    FOREIGN KEY (registration_number) REFERENCES horses_master(registration_number),
    FOREIGN KEY (trainer_id) REFERENCES trainers(external_party_id),
    FOREIGN KEY (jockey_id) REFERENCES trainers(external_party_id), -- Using same table for jockeys
    FOREIGN KEY (owner_id) REFERENCES owners(external_party_id),
    
    -- Unique constraint
    UNIQUE (race_id, registration_number)
);

-- ============================================================================
-- JUNCTION TABLES FOR MANY-TO-MANY RELATIONSHIPS
-- ============================================================================

-- Equipment details (supports multiple equipment per horse per race)
CREATE TABLE IF NOT EXISTS horse_race_equipment (
    race_id VARCHAR(100),
    registration_number VARCHAR(20),
    equipment_code VARCHAR(20),
    equipment_description VARCHAR(100),
    is_first_time BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (race_id, registration_number, equipment_code),
    FOREIGN KEY (race_id) REFERENCES races_standardized(race_id),
    FOREIGN KEY (registration_number) REFERENCES horses_master(registration_number),
    FOREIGN KEY (equipment_code) REFERENCES equipment_types(code)
);

-- Medication details  
CREATE TABLE IF NOT EXISTS horse_race_medication (
    race_id VARCHAR(100),
    registration_number VARCHAR(20),
    medication_code VARCHAR(20),
    medication_description VARCHAR(100),
    dosage VARCHAR(50),
    PRIMARY KEY (race_id, registration_number, medication_code),
    FOREIGN KEY (race_id) REFERENCES races_standardized(race_id),
    FOREIGN KEY (registration_number) REFERENCES horses_master(registration_number)
);

-- ============================================================================
-- PACE AND FRACTIONAL TIME TABLES
-- ============================================================================

-- Race pace and fractional times
CREATE TABLE IF NOT EXISTS race_fractions (
    race_id VARCHAR(100),
    call_position INTEGER, -- 1=first call, 2=second call, etc.
    distance_yards INTEGER,
    fraction_time DECIMAL(8,3),
    leader_at_call VARCHAR(20), -- registration_number of leader
    PRIMARY KEY (race_id, call_position),
    FOREIGN KEY (race_id) REFERENCES races_standardized(race_id),
    FOREIGN KEY (leader_at_call) REFERENCES horses_master(registration_number)
);

-- Individual horse position calls
CREATE TABLE IF NOT EXISTS horse_position_calls (
    race_id VARCHAR(100),
    registration_number VARCHAR(20),
    call_position INTEGER, -- 1=first call, 2=second call, etc.
    position INTEGER,
    lengths_behind DECIMAL(6,2),
    PRIMARY KEY (race_id, registration_number, call_position),
    FOREIGN KEY (race_id) REFERENCES races_standardized(race_id),
    FOREIGN KEY (registration_number) REFERENCES horses_master(registration_number)
);

-- ============================================================================
-- WAGERING AND PAYOUT TABLES
-- ============================================================================

-- Exotic wagering pools and payouts
CREATE TABLE IF NOT EXISTS race_wagering (
    race_id VARCHAR(100),
    wager_type VARCHAR(50), -- 'exacta', 'trifecta', 'superfecta', etc.
    pool_total DECIMAL(12,2),
    winning_combinations TEXT, -- JSON or comma-separated
    payout DECIMAL(10,2),
    number_of_winners INTEGER,
    PRIMARY KEY (race_id, wager_type),
    FOREIGN KEY (race_id) REFERENCES races_standardized(race_id)
);

-- ============================================================================
-- POPULATE REFERENCE TABLES WITH INITIAL DATA
-- ============================================================================

-- Course types
INSERT OR IGNORE INTO course_types (code, description, surface_category) VALUES
('DIRT', 'Dirt Track', 'dirt'),
('TURF', 'Turf Course', 'turf'),
('SYNTHETIC', 'Synthetic Surface', 'synthetic'),
('UNKNOWN', 'Unknown Surface', 'unknown');

-- Race types with hierarchy
INSERT OR IGNORE INTO race_types (code, description, class_level, purse_category) VALUES
('G1', 'Grade 1 Stakes', 10, 'GRADED_STAKES'),
('G2', 'Grade 2 Stakes', 9, 'GRADED_STAKES'),
('G3', 'Grade 3 Stakes', 8, 'GRADED_STAKES'),
('L', 'Listed Stakes', 7, 'STAKES'),
('STAKES', 'Stakes Race', 6, 'STAKES'),
('ALLOWANCE', 'Allowance Race', 5, 'ALLOWANCE'),
('N1X', 'Non-Winners of 1 Race Other Than', 4, 'ALLOWANCE'),
('N2X', 'Non-Winners of 2 Races Other Than', 3, 'ALLOWANCE'),
('CLAIMING', 'Claiming Race', 2, 'CLAIMING'),
('MAIDEN', 'Maiden Race', 1, 'MAIDEN'),
('OTHER', 'Other Race Type', 3, 'OTHER'),
('UNKNOWN', 'Unknown Race Type', 0, 'UNKNOWN');

-- Equipment types
INSERT OR IGNORE INTO equipment_types (code, description, equipment_category) VALUES
('BLINKERS', 'Blinkers', 'vision'),
('BLINKERS_FIRST_TIME', 'Blinkers First Time', 'vision'),
('TONGUE_TIE', 'Tongue Tie', 'respiratory'),
('NASAL_STRIP', 'Nasal Strip', 'respiratory'),
('SHADOW_ROLL', 'Shadow Roll', 'vision'),
('CHEEK_PIECES', 'Cheek Pieces', 'vision'),
('EAR_PLUGS', 'Ear Plugs', 'sensory'),
('HOOD', 'Hood', 'vision'),
('LASIX', 'Lasix (Furosemide)', 'medication'),
('LASIX_FIRST_TIME', 'Lasix First Time', 'medication'),
('LASIX_SECOND_TIME', 'Lasix Second Time', 'medication');

-- Track conditions
INSERT OR IGNORE INTO track_conditions (code, description, surface_speed, bias_tendency) VALUES
('FAST', 'Fast', 'fast', 'neutral'),
('GOOD', 'Good', 'average', 'neutral'),
('SLOPPY', 'Sloppy', 'slow', 'speed'),
('MUDDY', 'Muddy', 'slow', 'closer'),
('WET_FAST', 'Wet Fast', 'average', 'speed'),
('FIRM', 'Firm', 'fast', 'neutral'),
('YIELDING', 'Yielding', 'slow', 'closer'),
('SOFT', 'Soft', 'slow', 'closer'),
('HEAVY', 'Heavy', 'slow', 'closer'),
('OTHER', 'Other Condition', 'average', 'neutral'),
('UNKNOWN', 'Unknown Condition', 'average', 'neutral');

-- ============================================================================
-- CREATE INDEXES FOR PERFORMANCE
-- ============================================================================

-- Indexes for races_standardized
CREATE INDEX IF NOT EXISTS idx_race_date ON races_standardized(race_date);
CREATE INDEX IF NOT EXISTS idx_track_date ON races_standardized(track_code, race_date);
CREATE INDEX IF NOT EXISTS idx_race_type ON races_standardized(race_type_code);
CREATE INDEX IF NOT EXISTS idx_class_level ON races_standardized(class_level);
CREATE INDEX IF NOT EXISTS idx_distance ON races_standardized(distance_yards);
CREATE INDEX IF NOT EXISTS idx_purse ON races_standardized(purse_usd);

-- Indexes for race_entries_standardized
CREATE INDEX IF NOT EXISTS idx_race_entry ON race_entries_standardized(race_id, registration_number);
CREATE INDEX IF NOT EXISTS idx_horse_entries ON race_entries_standardized(registration_number);
CREATE INDEX IF NOT EXISTS idx_trainer_entries ON race_entries_standardized(trainer_id);
CREATE INDEX IF NOT EXISTS idx_finish_position ON race_entries_standardized(official_finish_position);
CREATE INDEX IF NOT EXISTS idx_claim_price ON race_entries_standardized(claim_price);
CREATE INDEX IF NOT EXISTS idx_odds ON race_entries_standardized(actual_odds);

-- ============================================================================
-- VIEWS FOR COMMON FEATURE ENGINEERING QUERIES
-- ============================================================================

-- Complete race entry view with all standardized features
CREATE VIEW IF NOT EXISTS vw_race_entries_complete AS
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
    r.min_age,
    r.max_age,
    r.fillies_and_mares,
    r.colts_and_geldings,
    h.horse_name,
    h.foaling_date,
    h.year_of_birth,
    h.sex_code,
    h.color_code,
    t.first_name as trainer_first_name,
    t.last_name as trainer_last_name,
    o.first_name as owner_first_name,
    o.last_name as owner_last_name
FROM race_entries_standardized re
JOIN races_standardized r ON re.race_id = r.race_id
JOIN horses_master h ON re.registration_number = h.registration_number
LEFT JOIN trainers t ON re.trainer_id = t.external_party_id
LEFT JOIN owners o ON re.owner_id = o.external_party_id;

-- Horse performance summary view
CREATE VIEW IF NOT EXISTS vw_horse_performance_summary AS
SELECT 
    registration_number,
    COUNT(*) as total_starts,
    SUM(CASE WHEN official_finish_position = 1 THEN 1 ELSE 0 END) as wins,
    SUM(CASE WHEN official_finish_position <= 2 THEN 1 ELSE 0 END) as win_place,
    SUM(CASE WHEN official_finish_position <= 3 THEN 1 ELSE 0 END) as win_place_show,
    AVG(official_finish_position) as avg_finish_position,
    SUM(win_payoff) as total_winnings,
    AVG(actual_odds) as avg_odds,
    MAX(race_date) as last_race_date,
    AVG(speed_rating) as avg_speed_rating
FROM vw_race_entries_complete 
WHERE official_finish_position IS NOT NULL
GROUP BY registration_number;