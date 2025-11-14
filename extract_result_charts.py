#!/usr/bin/env python3
"""
Result Chart Data Extraction Script
Extracts post-race results from Result Chart XML files into standardized database
"""

import sqlite3
import xml.etree.ElementTree as ET
import os
import glob
import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Manager, Lock
import time
from datetime import datetime
import traceback
from typing import Dict, List, Tuple, Optional
import zipfile
from standardization import RacingDataStandardizer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rc_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ResultChartExtractor:
    """Extracts result chart data with standardization"""
    
    def __init__(self, db_path: str = 'racing_data.db', max_workers: int = 45):
        self.db_path = db_path
        self.max_workers = max_workers
        self.standardizer = RacingDataStandardizer()
        self.manager = Manager()
        self.stats = self.manager.dict({
            'files_processed': 0,
            'races_updated': 0,
            'entries_updated': 0,
            'wagering_records': 0,
            'fraction_records': 0,
            'errors': 0
        })
        self.lock = Lock()
        
        # Batch data containers
        self.race_updates = self.manager.list()
        self.entry_updates = self.manager.list()
        self.wagering_batch = self.manager.list()
        self.fraction_batch = self.manager.list()
        self.position_calls_batch = self.manager.list()
        
        self.batch_size = 500
        
    def get_xml_files(self, directory: str) -> List[str]:
        """Get all XML files from directory (including in zip files)"""
        xml_files = []
        
        # Direct XML files  
        xml_pattern = os.path.join(directory, "*.xml")
        xml_files.extend(glob.glob(xml_pattern))
        
        # XML files in zip archives
        zip_pattern = os.path.join(directory, "*.zip")
        zip_files = glob.glob(zip_pattern)
        
        for zip_file in zip_files:
            try:
                with zipfile.ZipFile(zip_file, 'r') as zf:
                    for file_info in zf.infolist():
                        if file_info.filename.endswith('.xml') and not file_info.filename.startswith('__MACOSX'):
                            xml_files.append((zip_file, file_info.filename))
            except Exception as e:
                logger.warning(f"Could not read zip file {zip_file}: {e}")
        
        logger.info(f"Found {len(xml_files)} Result Chart XML files to process")
        return xml_files
    
    def extract_text(self, element, xpath: str) -> Optional[str]:
        """Safely extract text from XML element"""
        try:
            found = element.find(xpath)
            if found is not None and found.text:
                return found.text.strip()
            return None
        except:
            return None
    
    def extract_race_updates(self, chart_element, filename: str) -> Optional[Dict]:
        """Extract race result updates from chart element"""
        try:
            race_date = chart_element.get('RACE_DATE')
            if not race_date:
                return None
                
            track_element = chart_element.find('TRACK')
            if track_element is None:
                return None
                
            track_code = self.extract_text(track_element, 'CODE')
            if not track_code:
                return None
            
            # Process each race
            race_updates = []
            for race_element in chart_element.findall('RACE'):
                race_number = race_element.get('NUMBER')
                if not race_number:
                    continue
                
                race_id = f"{track_code}_{race_date}_{race_number}"
                
                # Extract result-specific data
                race_update = {
                    'race_id': race_id,
                    'winning_time': self.parse_time(self.extract_text(race_element, 'WIN_TIME')),
                    'final_fraction_time': self.parse_time(self.extract_text(race_element, 'FRACTION_5')),
                    'track_condition': self.standardizer.standardize_track_condition(
                        self.extract_text(race_element, 'TRK_COND')
                    ),
                    'weather': self.extract_text(race_element, 'WEATHER'),
                    'wind_speed': self.parse_numeric(self.extract_text(race_element, 'WIND_SPEED')),
                    'wind_direction': self.extract_text(race_element, 'WIND_DIRECTION')
                }
                
                # Extract fractions
                fractions = self.extract_race_fractions(race_element, race_id)
                if fractions:
                    self.fraction_batch.extend(fractions)
                
                # Extract wagering data
                wagering_data = self.extract_wagering_data(race_element, race_id)
                if wagering_data:
                    self.wagering_batch.extend(wagering_data)
                
                race_updates.append(race_update)
            
            return race_updates
            
        except Exception as e:
            logger.error(f"Error extracting race updates: {e}")
            return None
    
    def extract_race_fractions(self, race_element, race_id: str) -> List[Dict]:
        """Extract fractional times from race"""
        fractions = []
        
        try:
            # Extract fraction times (FRACTION_1 through FRACTION_5)
            for i in range(1, 6):
                fraction_time = self.parse_time(self.extract_text(race_element, f'FRACTION_{i}'))
                if fraction_time:
                    fraction_record = {
                        'race_id': race_id,
                        'call_position': i,
                        'distance_yards': self.get_fraction_distance(i),  # Approximate distances
                        'fraction_time': fraction_time,
                        'leader_at_call': None  # Could be enhanced to find leader
                    }
                    fractions.append(fraction_record)
        
        except Exception as e:
            logger.error(f"Error extracting fractions for {race_id}: {e}")
        
        return fractions
    
    def get_fraction_distance(self, call_position: int) -> Optional[int]:
        """Get approximate distance in yards for fraction call"""
        # These are typical distances, would need race-specific data for accuracy
        distances = {
            1: 440,   # 2 furlongs
            2: 880,   # 4 furlongs  
            3: 1320,  # 6 furlongs
            4: 1760,  # 1 mile
            5: 2200   # 1.25 miles
        }
        return distances.get(call_position)
    
    def extract_wagering_data(self, race_element, race_id: str) -> List[Dict]:
        """Extract exotic wagering pools and payouts"""
        wagering_records = []
        
        try:
            exotic_wagers = race_element.find('EXOTIC_WAGERS')
            if exotic_wagers is not None:
                for wager in exotic_wagers.findall('WAGER'):
                    wager_type = self.extract_text(wager, 'WAGER_TYPE')
                    if wager_type:
                        wagering_record = {
                            'race_id': race_id,
                            'wager_type': wager_type,
                            'pool_total': self.parse_numeric(self.extract_text(wager, 'POOL_TOTAL')),
                            'winning_combinations': self.extract_text(wager, 'WINNERS'),
                            'payout': self.parse_numeric(self.extract_text(wager, 'PAYOFF')),
                            'number_of_winners': self.parse_numeric(self.extract_text(wager, 'NUM_TICKETS'))
                        }
                        wagering_records.append(wagering_record)
        
        except Exception as e:
            logger.error(f"Error extracting wagering data for {race_id}: {e}")
        
        return wagering_records
    
    def extract_entry_updates(self, chart_element, filename: str) -> List[Dict]:
        """Extract entry result updates"""
        entry_updates = []
        
        try:
            race_date = chart_element.get('RACE_DATE')
            track_element = chart_element.find('TRACK')
            track_code = self.extract_text(track_element, 'CODE')
            
            for race_element in chart_element.findall('RACE'):
                race_number = race_element.get('NUMBER')
                race_id = f"{track_code}_{race_date}_{race_number}"
                
                # Process each entry
                for entry_element in race_element.findall('ENTRY'):
                    horse_name = self.extract_text(entry_element, 'NAME')
                    if not horse_name:
                        continue
                    
                    # Find registration number by horse name lookup
                    # (This requires a database query - could be optimized)
                    registration_number = self.lookup_registration_number(horse_name)
                    if not registration_number:
                        logger.warning(f"Could not find registration number for horse: {horse_name}")
                        continue
                    
                    entry_id = f"{race_id}_{registration_number}"
                    
                    # Extract result data
                    entry_update = {
                        'entry_id': entry_id,
                        'race_id': race_id,
                        'registration_number': registration_number,
                        'official_finish_position': self.parse_numeric(self.extract_text(entry_element, 'OFFICIAL_FIN')),
                        'final_time': self.parse_time(self.extract_text(entry_element, 'FINISH_TIME')),
                        'speed_rating': self.parse_numeric(self.extract_text(entry_element, 'SPEED_RATING')),
                        'win_payoff': self.parse_numeric(self.extract_text(entry_element, 'WIN_PAYOFF')),
                        'place_payoff': self.parse_numeric(self.extract_text(entry_element, 'PLACE_PAYOFF')),
                        'show_payoff': self.parse_numeric(self.extract_text(entry_element, 'SHOW_PAYOFF')),
                        'actual_odds': self.parse_numeric(self.extract_text(entry_element, 'DOLLAR_ODDS')),
                        'race_comments': self.extract_text(entry_element, 'COMMENT'),
                        'jockey_id': self.extract_text(entry_element, 'JOCKEY/KEY'),
                        'trainer_id': self.extract_text(entry_element, 'TRAINER/KEY')
                    }
                    
                    # Extract position calls
                    position_calls = self.extract_position_calls(entry_element, race_id, registration_number)
                    if position_calls:
                        self.position_calls_batch.extend(position_calls)
                    
                    entry_updates.append(entry_update)
        
        except Exception as e:
            logger.error(f"Error extracting entry updates: {e}")
        
        return entry_updates
    
    def extract_position_calls(self, entry_element, race_id: str, registration_number: str) -> List[Dict]:
        """Extract position calls for individual horse"""
        position_calls = []
        
        try:
            for call_element in entry_element.findall('POINT_OF_CALL'):
                which_call = call_element.get('WHICH')
                position = self.parse_numeric(self.extract_text(call_element, 'POSITION'))
                lengths_behind = self.parse_numeric(self.extract_text(call_element, 'LENGTHS'))
                
                if which_call and position is not None:
                    call_record = {
                        'race_id': race_id,
                        'registration_number': registration_number,
                        'call_position': self.map_call_position(which_call),
                        'position': position,
                        'lengths_behind': lengths_behind
                    }
                    position_calls.append(call_record)
        
        except Exception as e:
            logger.error(f"Error extracting position calls: {e}")
        
        return position_calls
    
    def map_call_position(self, which_call: str) -> int:
        """Map call description to numeric position"""
        call_mapping = {
            '1': 1,
            '2': 2,
            '3': 3,
            '4': 4,
            '5': 5,
            'FINAL': 6
        }
        return call_mapping.get(which_call, int(which_call) if which_call.isdigit() else 0)
    
    def lookup_registration_number(self, horse_name: str) -> Optional[str]:
        """Look up registration number by horse name"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT registration_number FROM horses_master 
                WHERE horse_name = ? 
                ORDER BY year_of_birth DESC 
                LIMIT 1
            """, (horse_name,))
            
            result = cursor.fetchone()
            conn.close()
            
            return result[0] if result else None
            
        except Exception as e:
            logger.error(f"Error looking up registration number for {horse_name}: {e}")
            return None
    
    def parse_time(self, time_str: Optional[str]) -> Optional[float]:
        """Parse time string to decimal seconds"""
        if not time_str:
            return None
        
        try:
            # Handle different time formats
            time_str = time_str.strip()
            
            # Format: MM:SS.ss or SS.ss
            if ':' in time_str:
                parts = time_str.split(':')
                minutes = float(parts[0])
                seconds = float(parts[1])
                return minutes * 60 + seconds
            else:
                return float(time_str)
                
        except (ValueError, TypeError):
            return None
    
    def parse_numeric(self, value_str: Optional[str]) -> Optional[float]:
        """Parse numeric string, handling various formats"""
        if not value_str:
            return None
        
        try:
            # Remove common non-numeric characters
            cleaned = str(value_str).replace(',', '').replace('$', '').strip()
            if cleaned == '' or cleaned == 'N/A':
                return None
            return float(cleaned)
        except (ValueError, TypeError):
            return None
    
    def parse_odds(self, odds_str: Optional[str]) -> Optional[float]:
        """Parse odds to decimal format"""
        if not odds_str:
            return None
        
        try:
            odds_str = odds_str.strip()
            if '/' in odds_str:
                parts = odds_str.split('/')
                return float(parts[0]) / float(parts[1])
            else:
                return float(odds_str)
        except (ValueError, TypeError, ZeroDivisionError):
            return None
    
    def process_xml_content(self, xml_content: str, filename: str) -> Tuple[int, int]:
        """Process XML content and extract result data"""
        races_count = 0
        entries_count = 0
        
        try:
            root = ET.fromstring(xml_content)
            
            # Extract race updates
            race_updates = self.extract_race_updates(root, filename)
            if race_updates:
                self.race_updates.extend(race_updates)
                races_count = len(race_updates)
            
            # Extract entry updates
            entry_updates = self.extract_entry_updates(root, filename)
            if entry_updates:
                self.entry_updates.extend(entry_updates)
                entries_count = len(entry_updates)
                
        except ET.ParseError as e:
            logger.error(f"XML parse error in {filename}: {e}")
            self.stats['errors'] += 1
        except Exception as e:
            logger.error(f"Unexpected error processing {filename}: {e}")
            logger.error(traceback.format_exc())
            self.stats['errors'] += 1
            
        return races_count, entries_count
    
    def process_file(self, file_path) -> None:
        """Process a single XML file"""
        try:
            if isinstance(file_path, tuple):
                # Handle zip file entry
                zip_path, xml_filename = file_path
                with zipfile.ZipFile(zip_path, 'r') as zf:
                    with zf.open(xml_filename) as xml_file:
                        xml_content = xml_file.read().decode('utf-8')
                filename = f"{zip_path}:{xml_filename}"
            else:
                # Handle direct XML file
                with open(file_path, 'r', encoding='utf-8') as f:
                    xml_content = f.read()
                filename = file_path
            
            races, entries = self.process_xml_content(xml_content, filename)
            
            with self.lock:
                self.stats['files_processed'] += 1
                self.stats['races_updated'] += races
                self.stats['entries_updated'] += entries
                
                if self.stats['files_processed'] % 100 == 0:
                    logger.info(f"Processed {self.stats['files_processed']} files. "
                              f"Race updates: {self.stats['races_updated']}, "
                              f"Entry updates: {self.stats['entries_updated']}")
                    
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            with self.lock:
                self.stats['errors'] += 1
    
    def batch_update_data(self):
        """Update database with result data"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Update races with result data
            if self.race_updates:
                for race_update in list(self.race_updates):
                    cursor.execute("""
                        UPDATE races_standardized 
                        SET winning_time = ?, final_fraction_time = ?, track_condition = ?,
                            weather = ?, wind_speed = ?, wind_direction = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE race_id = ?
                    """, (
                        race_update.get('winning_time'),
                        race_update.get('final_fraction_time'),
                        race_update.get('track_condition'),
                        race_update.get('weather'),
                        race_update.get('wind_speed'),
                        race_update.get('wind_direction'),
                        race_update['race_id']
                    ))
                
                logger.info(f"Updated {len(self.race_updates)} races with results")
            
            # Update entries with result data
            if self.entry_updates:
                for entry_update in list(self.entry_updates):
                    cursor.execute("""
                        UPDATE race_entries_standardized
                        SET official_finish_position = ?, final_time = ?, speed_rating = ?,
                            win_payoff = ?, place_payoff = ?, show_payoff = ?,
                            actual_odds = ?, race_comments = ?,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE entry_id = ?
                    """, (
                        entry_update.get('official_finish_position'),
                        entry_update.get('final_time'),
                        entry_update.get('speed_rating'),
                        entry_update.get('win_payoff'),
                        entry_update.get('place_payoff'),
                        entry_update.get('show_payoff'),
                        entry_update.get('actual_odds'),
                        entry_update.get('race_comments'),
                        entry_update['entry_id']
                    ))
                
                logger.info(f"Updated {len(self.entry_updates)} entries with results")
            
            # Insert wagering data
            if self.wagering_batch:
                wagering_tuples = [
                    (w['race_id'], w['wager_type'], w['pool_total'],
                     w['winning_combinations'], w['payout'], w['number_of_winners'])
                    for w in list(self.wagering_batch)
                ]
                
                cursor.executemany("""
                    INSERT OR IGNORE INTO race_wagering
                    (race_id, wager_type, pool_total, winning_combinations, payout, number_of_winners)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, wagering_tuples)
                
                logger.info(f"Inserted {len(wagering_tuples)} wagering records")
            
            # Insert fraction data
            if self.fraction_batch:
                fraction_tuples = [
                    (f['race_id'], f['call_position'], f['distance_yards'],
                     f['fraction_time'], f['leader_at_call'])
                    for f in list(self.fraction_batch)
                ]
                
                cursor.executemany("""
                    INSERT OR IGNORE INTO race_fractions
                    (race_id, call_position, distance_yards, fraction_time, leader_at_call)
                    VALUES (?, ?, ?, ?, ?)
                """, fraction_tuples)
                
                logger.info(f"Inserted {len(fraction_tuples)} fraction records")
            
            # Insert position calls
            if self.position_calls_batch:
                position_tuples = [
                    (p['race_id'], p['registration_number'], p['call_position'],
                     p['position'], p['lengths_behind'])
                    for p in list(self.position_calls_batch)
                ]
                
                cursor.executemany("""
                    INSERT OR IGNORE INTO horse_position_calls
                    (race_id, registration_number, call_position, position, lengths_behind)
                    VALUES (?, ?, ?, ?, ?)
                """, position_tuples)
                
                logger.info(f"Inserted {len(position_tuples)} position call records")
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Database update error: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def run_extraction(self, rc_directory: str = "2023 Result Charts") -> None:
        """Run the full Result Chart extraction process"""
        start_time = time.time()
        logger.info(f"Starting Result Chart extraction from {rc_directory}")
        logger.info(f"Using {self.max_workers} workers with result updates")
        
        # Get all XML files
        xml_files = self.get_xml_files(rc_directory)
        if not xml_files:
            logger.error(f"No XML files found in {rc_directory}")
            return
        
        # Process files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.process_file, xml_file) for xml_file in xml_files]
            
            for i, future in enumerate(futures):
                try:
                    future.result()
                    
                    # Batch update when we have enough data
                    if len(self.race_updates) >= self.batch_size:
                        logger.info("Performing batch database update...")
                        self.batch_update_data()
                        # Clear batches
                        self.race_updates[:] = []
                        self.entry_updates[:] = []
                        self.wagering_batch[:] = []
                        self.fraction_batch[:] = []
                        self.position_calls_batch[:] = []
                        
                except Exception as e:
                    logger.error(f"Future result error: {e}")
        
        # Final batch update
        if (self.race_updates or self.entry_updates or self.wagering_batch or 
            self.fraction_batch or self.position_calls_batch):
            logger.info("Final batch database update...")
            self.batch_update_data()
        
        # Final statistics
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("RESULT CHART EXTRACTION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Files processed: {self.stats['files_processed']}")
        logger.info(f"Races updated: {self.stats['races_updated']}")
        logger.info(f"Entries updated: {self.stats['entries_updated']}")
        logger.info(f"Wagering records: {len(self.wagering_batch)}")
        logger.info(f"Fraction records: {len(self.fraction_batch)}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Files per second: {self.stats['files_processed'] / duration:.2f}")
        logger.info("=" * 60)

if __name__ == "__main__":
    # Initialize extractor
    extractor = ResultChartExtractor(max_workers=45)
    
    # Run extraction
    extractor.run_extraction()