#!/usr/bin/env python3
"""
Enhanced Database Creation Script
Creates standardized racing database schema with reference tables
"""

import sqlite3
import logging
from standardization import RacingDataStandardizer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_enhanced_database(db_path='racing_data.db'):
    """Create enhanced database schema with standardization tables"""
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    logger.info("Creating enhanced racing database schema...")
    
    try:
        # Read and execute enhanced schema
        with open('enhanced_schema.sql', 'r') as sql_file:
            sql_commands = sql_file.read()
        
        # Execute schema creation
        cursor.executescript(sql_commands)
        conn.commit()
        
        # Verify tables were created
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
        """)
        tables = cursor.fetchall()
        
        logger.info("Enhanced database created successfully!")
        logger.info("Tables created:")
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table[0]}")
            count = cursor.fetchone()[0]
            logger.info(f"  - {table[0]}: {count} records")
        
        # Show views
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='view'
            ORDER BY name
        """)
        views = cursor.fetchall()
        
        if views:
            logger.info("Views created:")
            for view in views:
                logger.info(f"  - {view[0]}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating enhanced database: {e}")
        conn.rollback()
        return False
        
    finally:
        conn.close()

def test_standardization():
    """Test the standardization functions"""
    logger.info("Testing standardization functions...")
    
    standardizer = RacingDataStandardizer()
    
    # Test race type standardization
    test_race_types = [
        "MAIDEN CLAIMING",
        "G1 STAKES", 
        "ALLOWANCE N1X",
        "CLAIMING $25,000"
    ]
    
    logger.info("Race type standardization tests:")
    for race_type in test_race_types:
        result = standardizer.standardize_race_type(race_type)
        logger.info(f"  '{race_type}' -> {result}")
    
    # Test age restriction parsing
    test_ages = ["3YO", "4U", "3-5", "4 AND UP"]
    
    logger.info("Age restriction parsing tests:")
    for age in test_ages:
        result = standardizer.parse_age_restrictions(age)
        logger.info(f"  '{age}' -> {result}")
    
    # Test equipment standardization
    test_equipment = ["B,L1", "BLINKERS, LASIX FIRST TIME", "T, NS"]
    
    logger.info("Equipment standardization tests:")
    for equipment in test_equipment:
        result = standardizer.standardize_equipment(equipment)
        logger.info(f"  '{equipment}' -> {result}")
    
    # Test distance conversion
    test_distances = [
        ("6", "F"),  # 6 furlongs
        ("1.25", "M"),  # 1.25 miles
        ("1320", "Y")  # 1320 yards
    ]
    
    logger.info("Distance conversion tests:")
    for distance, unit in test_distances:
        result = standardizer.parse_distance(distance, unit)
        logger.info(f"  {distance} {unit} -> {result} yards")

if __name__ == "__main__":
    # Create enhanced database
    success = create_enhanced_database()
    
    if success:
        # Test standardization functions
        test_standardization()
        logger.info("Enhanced database and standardization system ready!")
    else:
        logger.error("Failed to create enhanced database")