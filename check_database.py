#!/usr/bin/env python3
"""
Check database status and verify standardization system
"""

import sqlite3
from standardization import RacingDataStandardizer

def check_database_status():
    """Check current database status"""
    
    conn = sqlite3.connect('racing_data.db')
    cursor = conn.cursor()
    
    print("=== DATABASE STATUS ===")
    
    # Check horse data
    cursor.execute("SELECT COUNT(*) FROM horses_master")
    horse_count = cursor.fetchone()[0]
    print(f"Horses in master table: {horse_count:,}")
    
    # Check trainers and owners
    cursor.execute("SELECT COUNT(*) FROM trainers")
    trainer_count = cursor.fetchone()[0]
    print(f"Trainers: {trainer_count:,}")
    
    cursor.execute("SELECT COUNT(*) FROM owners")
    owner_count = cursor.fetchone()[0]
    print(f"Owners: {owner_count:,}")
    
    # Show race type hierarchy
    print("\n=== RACE TYPE HIERARCHY ===")
    cursor.execute("SELECT code, description, class_level FROM race_types ORDER BY class_level DESC")
    for row in cursor.fetchall():
        print(f"Level {row[2]:2d}: {row[0]:<12} - {row[1]}")
    
    # Show equipment types
    print("\n=== EQUIPMENT TYPES ===")
    cursor.execute("SELECT code, description, equipment_category FROM equipment_types ORDER BY equipment_category, code")
    for row in cursor.fetchall():
        print(f"{row[2]:<12}: {row[0]:<20} - {row[1]}")
    
    # Sample horse records
    print("\n=== SAMPLE HORSE RECORDS ===")
    cursor.execute("""
        SELECT registration_number, horse_name, year_of_birth, 
               breed_type, sex_code, sire_registration_number, dam_registration_number
        FROM horses_master 
        WHERE sire_registration_number IS NOT NULL 
        AND dam_registration_number IS NOT NULL
        LIMIT 3
    """)
    
    for row in cursor.fetchall():
        print(f"Horse: {row[1]} ({row[0]})")
        print(f"  Born: {row[2]}, {row[3]} {row[4]}")
        print(f"  Sire: {row[5]}, Dam: {row[6]}")
        print()
    
    conn.close()

def test_advanced_standardization():
    """Test advanced standardization scenarios"""
    
    standardizer = RacingDataStandardizer()
    
    print("=== ADVANCED STANDARDIZATION TESTS ===")
    
    # Test complex race data
    race_data = {
        'course_type': 'D',
        'race_type': 'MAIDEN SPECIAL WEIGHT',
        'age_restrictions': '3YO',
        'sex_restrictions': 'FILLIES AND MARES',
        'distance': '6',
        'distance_unit': 'F',
        'purse': '$50,000',
        'track_condition': 'FAST'
    }
    
    features = standardizer.create_standardized_race_features(race_data)
    
    print("Race standardization example:")
    for key, value in features.items():
        print(f"  {key}: {value}")
    
    print()
    
    # Test horse features
    horse_data = {
        'equipment': 'B,L1,T',
        'medication': 'LASIX',
        'weight': '118'
    }
    
    horse_features = standardizer.create_standardized_horse_features(horse_data)
    
    print("Horse standardization example:")
    for key, value in horse_features.items():
        print(f"  {key}: {value}")

if __name__ == "__main__":
    check_database_status()
    print("\n" + "="*50 + "\n")
    test_advanced_standardization()