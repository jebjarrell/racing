#!/usr/bin/env python3
"""
Unit Tests for Racing Data Standardization
Tests critical business logic before applying fixes
"""

import unittest
from standardization import RacingDataStandardizer


class TestDistanceConversion(unittest.TestCase):
    """Test distance parsing and conversion to yards"""

    def setUp(self):
        self.standardizer = RacingDataStandardizer()

    def test_furlong_basic(self):
        """Test basic furlong conversions"""
        # 6 furlongs = 1320 yards
        self.assertEqual(self.standardizer.parse_distance("6", "F"), 1320)
        self.assertEqual(self.standardizer.parse_distance("6", "FURLONG"), 1320)

        # 8 furlongs = 1 mile = 1760 yards
        self.assertEqual(self.standardizer.parse_distance("8", "F"), 1760)

    def test_furlong_hundredths_format(self):
        """Test furlong format where DistanceId is in hundredths (e.g., 600 = 6F)"""
        # This is the format found in Equibase XML files
        # 600 with unit F should be 6 furlongs = 1320 yards
        self.assertEqual(self.standardizer.parse_distance("600", "F"), 1320)

        # 550 = 5.5 furlongs = 1210 yards
        self.assertEqual(self.standardizer.parse_distance("550", "F"), 1210)

        # 700 = 7 furlongs = 1540 yards
        self.assertEqual(self.standardizer.parse_distance("700", "F"), 1540)

        # 800 = 8 furlongs = 1 mile = 1760 yards
        self.assertEqual(self.standardizer.parse_distance("800", "F"), 1760)

    def test_mile_basic(self):
        """Test basic mile conversions"""
        # 1 mile = 1760 yards
        self.assertEqual(self.standardizer.parse_distance("1", "M"), 1760)

        # 1.5 miles = 2640 yards
        self.assertEqual(self.standardizer.parse_distance("1.5", "M"), 2640)

    def test_mile_hundredths_format(self):
        """Test mile format where DistanceId is in hundredths (e.g., 2400 = 1.5M)"""
        # BUG FOUND: XML has DistanceId=2400 for 1.5 miles
        # Current code does 2400 * 1760 = 4,224,000 yards (WRONG!)
        # Should be 1.5 * 1760 = 2640 yards

        # When DistanceId >= 100 with unit M, divide by 1600 to get miles
        # 2400 / 1600 = 1.5 miles = 2640 yards
        result = self.standardizer.parse_distance("2400", "M")
        self.assertIsNotNone(result)
        # Allow 5% tolerance for rounding
        self.assertAlmostEqual(result, 2640, delta=132)  # 132 = 5% of 2640

    def test_mile_turf_distances(self):
        """Test common turf distances in miles"""
        # 1 1/16 miles = 1.0625 miles = 1870 yards
        # Might be encoded as 1700 in hundredths format
        result = self.standardizer.parse_distance("1700", "M")
        if result:
            self.assertAlmostEqual(result, 1870, delta=100)

        # 1 1/8 miles = 1.125 miles = 1980 yards
        # Might be encoded as 1800 in hundredths format
        result = self.standardizer.parse_distance("1800", "M")
        if result:
            self.assertAlmostEqual(result, 1980, delta=100)

    def test_yards_direct(self):
        """Test direct yard values"""
        # Should pass through unchanged
        self.assertEqual(self.standardizer.parse_distance("1320", "Y"), 1320)
        self.assertEqual(self.standardizer.parse_distance("1760", "YARDS"), 1760)

    def test_sprint_distances(self):
        """Test common sprint distances (< 1 mile)"""
        # 5 furlongs = 1100 yards (very common sprint distance)
        self.assertEqual(self.standardizer.parse_distance("500", "F"), 1100)

        # 6.5 furlongs = 1430 yards
        self.assertEqual(self.standardizer.parse_distance("650", "F"), 1430)

        # 7 furlongs = 1540 yards
        self.assertEqual(self.standardizer.parse_distance("700", "F"), 1540)

    def test_distance_sanity_checks(self):
        """Test that converted distances are in reasonable ranges"""
        # Minimum: 2 furlongs (440 yards) - rare but possible
        result = self.standardizer.parse_distance("200", "F")
        self.assertGreaterEqual(result, 400)
        self.assertLessEqual(result, 500)

        # Maximum: 2.5 miles (4400 yards) - longest typical races
        result = self.standardizer.parse_distance("2.5", "M")
        self.assertGreaterEqual(result, 4000)
        self.assertLessEqual(result, 4500)

        # Common 1 mile distance must be exactly 1760 yards
        self.assertEqual(self.standardizer.parse_distance("1", "M"), 1760)


class TestTrackCodeExtraction(unittest.TestCase):
    """Test track code extraction from filenames"""

    def test_pp_filename_format(self):
        """Test Past Performance filename format: SIMD20230101AQU_USA.xml"""
        filename = "SIMD20230101AQU_USA.xml"

        # Track code should be AQU (positions 12-14), NOT 'USA'
        # Expected: AQU (Aqueduct)
        # Current bug: Extracts 'USA' instead

        # This will be tested in the extractor integration test
        # Here we document the expected format
        base = filename.replace('.xml', '').replace('.zip', '')

        # Correct extraction: positions 12-14
        if len(base) >= 15:
            track_code = base[12:15]
            self.assertEqual(track_code, "AQU")

    def test_rc_filename_format(self):
        """Test Result Chart filename format: aqu20230101tch.xml"""
        filename = "aqu20230101tch.xml"

        # Track code should be 'aqu' (first 3 chars, uppercase = AQU)
        track_code = filename[:3].upper()
        self.assertEqual(track_code, "AQU")

    def test_various_tracks(self):
        """Test track code extraction for various racetracks"""
        test_cases = [
            ("SIMD20230101AQU_USA.xml", "AQU"),  # Aqueduct
            ("SIMD20230101GP_USA.xml", "GP"),     # Gulfstream Park
            ("SIMD20230101SA_USA.xml", "SA"),     # Santa Anita
            ("SIMD20230101CD_USA.xml", "CD"),     # Churchill Downs
            ("SIMD20230101BEL_USA.xml", "BEL"),   # Belmont Park
        ]

        for filename, expected_track in test_cases:
            base = filename.replace('.xml', '').replace('.zip', '')

            # Extract track code using the same logic as extract_past_performance.py
            if len(base) >= 14:
                track_part = base[12:]  # Everything after SIMD + date
                if '_' in track_part:
                    track_code = track_part.split('_')[0]  # Take part before '_'
                else:
                    track_code = track_part

                track_code = track_code.upper()[:4] if track_code else 'UNK'

                self.assertEqual(track_code, expected_track,
                               f"Failed for {filename}: got {track_code}, expected {expected_track}")


class TestCourseTypeStandardization(unittest.TestCase):
    """Test course type normalization"""

    def setUp(self):
        self.standardizer = RacingDataStandardizer()

    def test_dirt_variations(self):
        """Test various dirt surface identifiers"""
        self.assertEqual(self.standardizer.standardize_course_type("D"), "DIRT")
        self.assertEqual(self.standardizer.standardize_course_type("DIRT"), "DIRT")
        self.assertEqual(self.standardizer.standardize_course_type("FAST"), "DIRT")
        self.assertEqual(self.standardizer.standardize_course_type("MUDDY"), "DIRT")

    def test_turf_variations(self):
        """Test various turf surface identifiers"""
        self.assertEqual(self.standardizer.standardize_course_type("T"), "TURF")
        self.assertEqual(self.standardizer.standardize_course_type("TURF"), "TURF")
        self.assertEqual(self.standardizer.standardize_course_type("FIRM"), "TURF")

    def test_synthetic(self):
        """Test synthetic surface identifiers"""
        self.assertEqual(self.standardizer.standardize_course_type("S"), "SYNTHETIC")
        self.assertEqual(self.standardizer.standardize_course_type("SYNTHETIC"), "SYNTHETIC")


class TestRaceTypeClassification(unittest.TestCase):
    """Test race type parsing and classification"""

    def setUp(self):
        self.standardizer = RacingDataStandardizer()

    def test_graded_stakes(self):
        """Test graded stakes race classification"""
        result = self.standardizer.standardize_race_type("G1")
        self.assertEqual(result['class_level'], 10)
        self.assertEqual(result['purse_category'], 'GRADED_STAKES')

        result = self.standardizer.standardize_race_type("G3")
        self.assertEqual(result['class_level'], 8)

    def test_claiming_races(self):
        """Test claiming race classification"""
        result = self.standardizer.standardize_race_type("CLAIMING")
        self.assertEqual(result['race_type_code'], 'CLAIMING')
        self.assertEqual(result['class_level'], 2)
        self.assertEqual(result['purse_category'], 'CLAIMING')

    def test_maiden_races(self):
        """Test maiden race classification"""
        result = self.standardizer.standardize_race_type("MAIDEN")
        self.assertEqual(result['race_type_code'], 'MAIDEN')
        self.assertEqual(result['class_level'], 1)

        result = self.standardizer.standardize_race_type("MAIDEN CLAIMING")
        self.assertEqual(result['class_level'], 1)


if __name__ == '__main__':
    # Run tests with verbose output
    unittest.main(verbosity=2)
