#!/usr/bin/env python3
"""
Debug distance conversion specifically
"""

from standardization import RacingDataStandardizer

def test_distance_conversion():
    """Test specific distance cases"""
    
    standardizer = RacingDataStandardizer()
    
    test_cases = [
        ("600", "F"),  # 6 furlongs should be 1320 yards
        ("6", "F"),    # 6 furlongs should be 1320 yards  
        ("1320", "Y"), # 1320 yards should stay 1320
        ("1", "M"),    # 1 mile should be 1760 yards
    ]
    
    print("Distance conversion tests:")
    for distance, unit in test_cases:
        result = standardizer.parse_distance(distance, unit)
        print(f"  {distance} {unit} -> {result} yards")
    
    # Test the specific issue
    race_data = {
        'distance': '600',
        'distance_unit': 'F'
    }
    
    features = standardizer.create_standardized_race_features(race_data)
    print(f"\nStandardized race features:")
    print(f"  distance_yards: {features.get('distance_yards')}")

if __name__ == "__main__":
    test_distance_conversion()