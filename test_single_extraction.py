#!/usr/bin/env python3
"""
Quick test of extraction scripts on single files
"""

import xml.etree.ElementTree as ET
import sqlite3
import os
from extract_past_performance import PastPerformanceExtractor
from extract_result_charts import ResultChartExtractor

def test_pp_extraction():
    """Test Past Performance extraction on single file"""
    print("Testing Past Performance extraction...")
    
    extractor = PastPerformanceExtractor(
        db_path="test_racing.db",
        max_workers=1
    )
    
    # Test single file
    test_file = "2023 PPs/SIMD20230101AQU_USA.xml"
    if os.path.exists(test_file):
        print(f"Processing: {test_file}")
        try:
            extractor.process_file(test_file)
            print("PASS Past Performance extraction successful")
        except Exception as e:
            print(f"FAIL Past Performance extraction failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"FAIL Test file not found: {test_file}")

def test_rc_extraction():
    """Test Result Chart extraction on single file"""
    print("\nTesting Result Chart extraction...")
    
    extractor = ResultChartExtractor(
        db_path="test_racing.db",
        max_workers=1
    )
    
    # Test single file
    test_file = "2023 Result Charts/aqu20230101tch.xml"
    if os.path.exists(test_file):
        print(f"Processing: {test_file}")
        try:
            extractor.process_file(test_file)
            print("PASS Result Chart extraction successful")
        except Exception as e:
            print(f"FAIL Result Chart extraction failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"FAIL Test file not found: {test_file}")

def check_database():
    """Check what was extracted"""
    print("\nDatabase contents:")
    conn = sqlite3.connect("test_racing.db")
    cursor = conn.cursor()
    
    tables = ['races_standardized', 'race_entries_standardized', 'result_chart_entries', 'point_of_call_data']
    
    for table in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table}: {count} records")
        except:
            print(f"  {table}: table not found")
    
    conn.close()

if __name__ == "__main__":
    # Clean up any existing test database
    if os.path.exists("test_racing.db"):
        os.remove("test_racing.db")
    
    test_pp_extraction()
    test_rc_extraction()
    check_database()