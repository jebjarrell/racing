#!/usr/bin/env python3
"""
Test both extraction scripts on sample data
"""

import logging
from extract_past_performance import PastPerformanceExtractor
from extract_result_charts import ResultChartExtractor
import sqlite3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_past_performance_extraction():
    """Test Past Performance extraction on a sample file"""
    
    logger.info("Testing Past Performance extraction...")
    
    # Initialize extractor with minimal workers for testing
    extractor = PastPerformanceExtractor(max_workers=1)
    
    # Test on the sample file
    test_file = "2023 PPs/SIMD20230101AQU_USA.xml"
    
    try:
        # Read and process the test file
        with open(test_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        races, entries = extractor.process_xml_content(xml_content, test_file)
        
        logger.info(f"Past Performance extraction results:")
        logger.info(f"  Races found: {races}")
        logger.info(f"  Entries found: {entries}")
        
        if extractor.race_batch:
            logger.info(f"\nSample race data:")
            sample_race = dict(extractor.race_batch[0])
            for key, value in sample_race.items():
                logger.info(f"  {key}: {value}")
        
        if extractor.entry_batch:
            logger.info(f"\nSample entry data:")
            sample_entry = dict(extractor.entry_batch[0])
            for key, value in sample_entry.items():
                logger.info(f"  {key}: {value}")
        
        # Test database insertion
        logger.info(f"\nTesting database insertion...")
        extractor.batch_insert_data()
        
        return True
        
    except Exception as e:
        logger.error(f"Past Performance test failed: {e}")
        return False

def test_result_chart_extraction():
    """Test Result Chart extraction on a sample file"""
    
    logger.info("Testing Result Chart extraction...")
    
    # Check if we have result chart files
    try:
        test_file = "2023 Result Charts/aqu20230101tch.xml"
        
        # Initialize extractor with minimal workers for testing
        extractor = ResultChartExtractor(max_workers=1)
        
        # Read and process the test file
        with open(test_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        races, entries = extractor.process_xml_content(xml_content, test_file)
        
        logger.info(f"Result Chart extraction results:")
        logger.info(f"  Race updates: {races}")
        logger.info(f"  Entry updates: {entries}")
        
        if extractor.race_updates:
            logger.info(f"\nSample race update:")
            sample_race = dict(extractor.race_updates[0])
            for key, value in sample_race.items():
                logger.info(f"  {key}: {value}")
        
        if extractor.entry_updates:
            logger.info(f"\nSample entry update:")
            sample_entry = dict(extractor.entry_updates[0])
            for key, value in sample_entry.items():
                logger.info(f"  {key}: {value}")
        
        # Test database update
        logger.info(f"\nTesting database update...")
        extractor.batch_update_data()
        
        return True
        
    except FileNotFoundError:
        logger.warning("No Result Chart test file found - skipping test")
        return True
    except Exception as e:
        logger.error(f"Result Chart test failed: {e}")
        return False

def check_database_status():
    """Check database after extractions"""
    
    logger.info("Checking database status after extraction tests...")
    
    conn = sqlite3.connect('racing_data.db')
    cursor = conn.cursor()
    
    try:
        # Check races
        cursor.execute("SELECT COUNT(*) FROM races_standardized")
        race_count = cursor.fetchone()[0]
        logger.info(f"Races in database: {race_count}")
        
        # Check entries
        cursor.execute("SELECT COUNT(*) FROM race_entries_standardized")
        entry_count = cursor.fetchone()[0]
        logger.info(f"Entries in database: {entry_count}")
        
        # Check equipment
        cursor.execute("SELECT COUNT(*) FROM horse_race_equipment")
        equipment_count = cursor.fetchone()[0]
        logger.info(f"Equipment records: {equipment_count}")
        
        # Check wagering
        cursor.execute("SELECT COUNT(*) FROM race_wagering")
        wagering_count = cursor.fetchone()[0]
        logger.info(f"Wagering records: {wagering_count}")
        
        # Show sample complete race entry
        cursor.execute("""
            SELECT race_id, track_code, race_date, race_number, 
                   course_type_code, race_type_code, distance_yards, purse_usd
            FROM races_standardized 
            WHERE race_id IS NOT NULL
            LIMIT 1
        """)
        
        sample_race = cursor.fetchone()
        if sample_race:
            logger.info(f"\nSample race record:")
            columns = ['race_id', 'track_code', 'race_date', 'race_number', 
                      'course_type', 'race_type', 'distance_yards', 'purse']
            for i, col in enumerate(columns):
                logger.info(f"  {col}: {sample_race[i]}")
        
        # Show sample entry with standardized features
        cursor.execute("""
            SELECT entry_id, registration_number, program_number, post_position,
                   has_blinkers, has_lasix, weight_lbs, morning_line_odds,
                   official_finish_position, actual_odds
            FROM race_entries_standardized
            WHERE entry_id IS NOT NULL
            LIMIT 1
        """)
        
        sample_entry = cursor.fetchone()
        if sample_entry:
            logger.info(f"\nSample entry record:")
            columns = ['entry_id', 'registration_number', 'program_number', 'post_position',
                      'has_blinkers', 'has_lasix', 'weight_lbs', 'morning_line_odds',
                      'finish_position', 'actual_odds']
            for i, col in enumerate(columns):
                logger.info(f"  {col}: {sample_entry[i]}")
        
    except Exception as e:
        logger.error(f"Database check error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    success = True
    
    # Test Past Performance extraction
    if not test_past_performance_extraction():
        success = False
    
    print("\n" + "="*50 + "\n")
    
    # Test Result Chart extraction
    if not test_result_chart_extraction():
        success = False
    
    print("\n" + "="*50 + "\n")
    
    # Check final database status
    check_database_status()
    
    if success:
        logger.info("\n✅ All extraction script tests passed!")
    else:
        logger.error("\n❌ Some tests failed!")