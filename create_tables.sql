-- Racing Pipeline Database Schema
-- Creates tables for horse racing data extraction from Equibase XML files

-- Horses master table - primary entity for tracking horses across races
CREATE TABLE IF NOT EXISTS horses_master (
    registration_number VARCHAR(20) PRIMARY KEY,
    horse_name VARCHAR(255) NOT NULL,
    foaling_date DATE,
    year_of_birth INTEGER,
    foaling_area VARCHAR(10),
    breed_type VARCHAR(10),
    color_code VARCHAR(20),
    sex_code VARCHAR(5),
    breeder_name VARCHAR(500),
    sire_registration_number VARCHAR(20),
    dam_registration_number VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sire_registration_number) REFERENCES horses_master(registration_number),
    FOREIGN KEY (dam_registration_number) REFERENCES horses_master(registration_number)
);

-- Trainers table - tracks trainer information by ExternalPartyId
CREATE TABLE IF NOT EXISTS trainers (
    external_party_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    middle_name VARCHAR(100),
    last_name VARCHAR(100),
    type_source VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Owners table - tracks owner information by ExternalPartyId  
CREATE TABLE IF NOT EXISTS owners (
    external_party_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    middle_name VARCHAR(100),
    last_name VARCHAR(255),
    type_source VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_horses_name ON horses_master(horse_name);
CREATE INDEX IF NOT EXISTS idx_horses_year ON horses_master(year_of_birth);
CREATE INDEX IF NOT EXISTS idx_horses_sire ON horses_master(sire_registration_number);
CREATE INDEX IF NOT EXISTS idx_horses_dam ON horses_master(dam_registration_number);
CREATE INDEX IF NOT EXISTS idx_trainers_name ON trainers(last_name, first_name);
CREATE INDEX IF NOT EXISTS idx_owners_name ON owners(last_name, first_name);