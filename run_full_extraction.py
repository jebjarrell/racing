#!/usr/bin/env python3
"""
Run complete extraction pipeline
1. Extract horses/trainers/owners from Past Performance files
2. Extract race and entry data from Past Performance files  
3. Update with results from Result Chart files
"""

import logging
import time
from extract_horses import HorseExtractor
from extract_past_performance import PastPerformanceExtractor
from extract_result_charts import ResultChartExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_full_pipeline():
    """Run the complete extraction pipeline"""
    
    logger.info("="*60)
    logger.info("STARTING FULL RACING DATA EXTRACTION PIPELINE")
    logger.info("="*60)
    
    total_start = time.time()
    
    # Step 1: Extract horses, trainers, owners
    logger.info("\n🐎 STEP 1: Extracting horses, trainers, and owners...")
    horse_extractor = HorseExtractor(max_workers=45)
    horse_extractor.run_extraction()
    
    # Step 2: Extract Past Performance race/entry data
    logger.info("\n🏁 STEP 2: Extracting Past Performance race and entry data...")
    pp_extractor = PastPerformanceExtractor(max_workers=45)
    pp_extractor.run_extraction()
    
    # Step 3: Extract Result Chart data and update races
    logger.info("\n🏆 STEP 3: Extracting Result Chart data and updating with results...")
    rc_extractor = ResultChartExtractor(max_workers=45)
    rc_extractor.run_extraction()
    
    total_end = time.time()
    total_duration = total_end - total_start
    
    logger.info("="*60)
    logger.info("FULL PIPELINE COMPLETE")
    logger.info("="*60)
    logger.info(f"Total pipeline duration: {total_duration/60:.1f} minutes")
    logger.info("="*60)
    
    # Final database summary
    import sqlite3
    conn = sqlite3.connect('racing_data.db')
    cursor = conn.cursor()
    
    try:
        cursor.execute("SELECT COUNT(*) FROM horses_master")
        horses = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM races_standardized")
        races = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM race_entries_standardized")
        entries = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM race_wagering")
        wagering = cursor.fetchone()[0]
        
        logger.info("📊 FINAL DATABASE SUMMARY:")
        logger.info(f"   Horses: {horses:,}")
        logger.info(f"   Races: {races:,}")
        logger.info(f"   Entries: {entries:,}")
        logger.info(f"   Wagering records: {wagering:,}")
        
    except Exception as e:
        logger.error(f"Error getting final stats: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_full_pipeline()