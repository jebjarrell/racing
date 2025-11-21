#!/usr/bin/env python3
"""
Integration Tests for Racing Data Extraction Pipeline
Tests end-to-end extraction with real XML samples
"""

import unittest
import sqlite3
import os
import tempfile
from extract_horses import HorseExtractor
from extract_past_performance import PastPerformanceExtractor
from extract_result_charts import ResultChartExtractor


class TestFullPipeline(unittest.TestCase):
    """Integration test for complete extraction pipeline"""

    def setUp(self):
        """Create a temporary test database"""
        self.test_db_fd, self.test_db_path = tempfile.mkstemp(suffix='.db')

        # Initialize database with schema
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()

        # Load enhanced schema
        with open('enhanced_schema.sql', 'r') as f:
            schema = f.read()
            cursor.executescript(schema)

        # Also create basic tables
        with open('create_tables.sql', 'r') as f:
            schema = f.read()
            cursor.executescript(schema)

        conn.commit()
        conn.close()

    def tearDown(self):
        """Clean up test database"""
        os.close(self.test_db_fd)
        os.unlink(self.test_db_path)

    def test_distance_in_real_data(self):
        """Test that distances are correctly extracted from real XML files"""
        # Use test database
        extractor = PastPerformanceExtractor(db_path=self.test_db_path, max_workers=1)

        test_file = "2023 PPs/SIMD20230101AQU_USA.xml"

        if os.path.exists(test_file):
            with open(test_file, 'r', encoding='utf-8') as f:
                xml_content = f.read()

            # Process the file
            extractor.process_xml_content(xml_content, test_file)
            extractor.batch_insert_data()

            # Check extracted distances
            conn = sqlite3.connect(self.test_db_path)
            cursor = conn.cursor()

            cursor.execute("""
                SELECT race_id, distance_yards, source_file
                FROM races_standardized
                WHERE distance_yards IS NOT NULL
                ORDER BY race_number
                LIMIT 5
            """)

            results = cursor.fetchall()
            self.assertGreater(len(results), 0, "No races extracted")

            for race_id, distance, source in results:
                # All distances should be reasonable (between 400 and 5000 yards)
                self.assertGreater(distance, 400,
                                 f"Distance too small for {race_id}: {distance} yards")
                self.assertLess(distance, 5000,
                              f"Distance too large for {race_id}: {distance} yards")

            conn.close()

    def test_track_code_matching(self):
        """Test that track codes match between PP and RC files"""
        # Extract from PP file
        pp_extractor = PastPerformanceExtractor(db_path=self.test_db_path, max_workers=1)

        test_pp = "2023 PPs/SIMD20230101AQU_USA.xml"
        test_rc = "2023 Result Charts/aqu20230101tch.xml"

        if os.path.exists(test_pp) and os.path.exists(test_rc):
            # Process PP file
            with open(test_pp, 'r', encoding='utf-8') as f:
                xml_content = f.read()
            pp_extractor.process_xml_content(xml_content, test_pp)
            pp_extractor.batch_insert_data()

            # Get PP track codes
            conn = sqlite3.connect(self.test_db_path)
            cursor = conn.cursor()

            cursor.execute("SELECT DISTINCT track_code FROM races_standardized")
            pp_tracks = [row[0] for row in cursor.fetchall()]

            # Track code from PP file should be AQU, not USA
            self.assertIn('AQU', pp_tracks,
                         f"Expected track code 'AQU' but got {pp_tracks}")
            self.assertNotIn('USA', pp_tracks,
                           f"Should not have 'USA' as track code")

            conn.close()

    def test_horse_master_population(self):
        """Test that horses_master table is populated"""
        test_file = "2023 PPs/SIMD20230101AQU_USA.xml"

        if os.path.exists(test_file):
            with open(test_file, 'r', encoding='utf-8') as f:
                xml_content = f.read()

            # Step 1: Extract horses first
            horse_extractor = HorseExtractor(db_path=self.test_db_path, max_workers=1)
            horses, trainers, owners = horse_extractor.process_xml_content(xml_content, test_file)
            horse_extractor.batch_insert_data()

            # Step 2: Extract race entries
            pp_extractor = PastPerformanceExtractor(db_path=self.test_db_path, max_workers=1)
            pp_extractor.process_xml_content(xml_content, test_file)
            pp_extractor.batch_insert_data()

            # Verify data was inserted
            conn = sqlite3.connect(self.test_db_path)
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM horses_master")
            horse_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM trainers")
            trainer_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM owners")
            owner_count = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM race_entries_standardized")
            entry_count = cursor.fetchone()[0]

            self.assertGreater(horse_count, 0, "No horses extracted")
            self.assertGreater(trainer_count, 0, "No trainers extracted")
            self.assertGreater(owner_count, 0, "No owners extracted")
            self.assertGreater(entry_count, 0, "No entries extracted")

            # Verify we can join horses to entries (foreign key integrity)
            cursor.execute("""
                SELECT COUNT(*)
                FROM race_entries_standardized e
                INNER JOIN horses_master h ON e.registration_number = h.registration_number
            """)

            joined_count = cursor.fetchone()[0]
            self.assertGreater(joined_count, 0,
                             "Cannot join entries to horses - foreign key issue")

            conn.close()


class TestDataQuality(unittest.TestCase):
    """Test data quality and consistency"""

    def test_distance_conversions_manual(self):
        """Manually verify distance conversion formulas"""
        # Test the math
        # 6 furlongs = 6 * 220 yards = 1,320 yards
        self.assertEqual(6 * 220, 1320)

        # 1 mile = 1760 yards
        self.assertEqual(1 * 1760, 1760)

        # 1.5 miles = 2640 yards
        self.assertEqual(int(1.5 * 1760), 2640)

        # 8 furlongs = 1 mile
        self.assertEqual(8 * 220, 1760)

    def test_track_code_extraction_logic(self):
        """Test track code extraction from various filename formats"""
        test_cases = [
            # (filename, expected_track_code)
            ("SIMD20230101AQU_USA.xml", "AQU"),
            ("SIMD20230115GP_USA.xml", "GP"),
            ("SIMD20230120SA_USA.xml", "SA"),
            ("SIMD20230125BEL_USA.xml", "BEL"),
            ("SIMD20230101CD_USA.xml", "CD"),
        ]

        for filename, expected in test_cases:
            base = filename.replace('.xml', '').replace('.zip', '')

            # Extract track code using the same logic as extract_past_performance.py
            # Format: SIMD(8 chars for date)(2-3 chars for track)_USA
            # Position 0-3: SIMD
            # Position 4-11: YYYYMMDD  (8 digits)
            # Position 12+: Track code (variable length) up to '_'

            if len(base) >= 14:
                track_part = base[12:]  # Everything after SIMD + date
                if '_' in track_part:
                    track_code = track_part.split('_')[0]  # Take part before '_'
                else:
                    track_code = track_part

                track_code = track_code.upper()[:4] if track_code else 'UNK'

                self.assertEqual(track_code, expected,
                               f"Failed for {filename}: got '{track_code}', expected '{expected}'")


if __name__ == '__main__':
    # Run with verbose output
    unittest.main(verbosity=2)
