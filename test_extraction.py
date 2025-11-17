#!/usr/bin/env python3
"""
Test script to validate horse extraction before full run
"""

import sqlite3
from extract_horses import HorseExtractor
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_single_file():
    """Test extraction on a single file"""
    
    # Initialize extractor with minimal workers for testing
    extractor = HorseExtractor(max_workers=1)
    
    # Test on the sample file we've been analyzing
    test_file = "2023 PPs/SIMD20230101AQU_USA.xml"
    
    logger.info(f"Testing extraction on: {test_file}")
    
    try:
        # Read and process the test file
        with open(test_file, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        horses, trainers, owners = extractor.process_xml_content(xml_content, test_file)
        
        logger.info(f"Extraction results:")
        logger.info(f"  Horses found: {horses}")
        logger.info(f"  Trainers found: {trainers}")
        logger.info(f"  Owners found: {owners}")
        
        # Show sample data
        if extractor.horse_batch:
            logger.info(f"\nSample horse data:")
            sample_horse = dict(extractor.horse_batch[0])
            for key, value in sample_horse.items():
                logger.info(f"  {key}: {value}")
        
        if extractor.trainer_batch:
            logger.info(f"\nSample trainer data:")
            sample_trainer = dict(extractor.trainer_batch[0])
            for key, value in sample_trainer.items():
                logger.info(f"  {key}: {value}")
        
        if extractor.owner_batch:
            logger.info(f"\nSample owner data:")
            sample_owner = dict(extractor.owner_batch[0])
            for key, value in sample_owner.items():
                logger.info(f"  {key}: {value}")
        
        # Test database insertion
        logger.info("\nTesting database insertion...")
        extractor.batch_insert_data()
        
        # Verify data was inserted
        conn = sqlite3.connect('racing_data.db')
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM horses_master")
        horse_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM trainers")
        trainer_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM owners")
        owner_count = cursor.fetchone()[0]
        
        logger.info(f"Database verification:")
        logger.info(f"  Horses in DB: {horse_count}")
        logger.info(f"  Trainers in DB: {trainer_count}")
        logger.info(f"  Owners in DB: {owner_count}")
        
        # Show a sample horse record
        cursor.execute("SELECT * FROM horses_master LIMIT 1")
        sample_record = cursor.fetchone()
        if sample_record:
            logger.info(f"\nSample database record:")
            columns = [desc[0] for desc in cursor.description]
            for i, col in enumerate(columns):
                logger.info(f"  {col}: {sample_record[i]}")
        
        conn.close()
        
        logger.info("\n✅ Test completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    test_single_file()