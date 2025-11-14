#!/usr/bin/env python3
"""
Past Performance Data Extraction Script
Extracts pre-race data from Past Performance XML files into standardized database
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
        logging.FileHandler('pp_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class PastPerformanceExtractor:
    """Extracts past performance data with standardization"""
    
    def __init__(self, db_path: str = 'racing_data.db', max_workers: int = 45):
        self.db_path = db_path
        self.max_workers = max_workers
        self.standardizer = RacingDataStandardizer()
        self.manager = Manager()
        self.stats = self.manager.dict({
            'files_processed': 0,
            'races_extracted': 0,
            'entries_extracted': 0,
            'equipment_records': 0,
            'errors': 0
        })
        self.lock = Lock()
        
        # Batch data containers
        self.race_batch = self.manager.list()
        self.entry_batch = self.manager.list()
        self.equipment_batch = self.manager.list()
        
        # Deduplication tracking
        self.seen_races = self.manager.dict()
        
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
        
        logger.info(f"Found {len(xml_files)} Past Performance XML files to process")
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
    
    def extract_race_data(self, race_element, track_code: str, race_date: str, filename: str) -> Optional[Dict]:
        """Extract standardized race data from XML race element"""
        try:
            race_number = self.extract_text(race_element, 'RaceNumber')
            if not race_number:
                return None
            
            # Create race ID
            race_id = f"{track_code}_{race_date}_{race_number}"
            
            # Extract raw race data
            raw_race_data = {
                'course_type': self.extract_text(race_element, 'Course/CourseType/Value'),
                'race_type': self.extract_text(race_element, 'RaceType/Description'),
                'age_restrictions': self.extract_text(race_element, 'AgeRestriction/Value'),
                'sex_restrictions': self.extract_text(race_element, 'SexRestriction/Value'),
                'distance': self.extract_text(race_element, 'Distance/DistanceId'),
                'distance_unit': self.extract_text(race_element, 'Distance/DistanceUnit/Value'),
                'purse': self.extract_text(race_element, 'PurseUSA'),
                'track_condition': None  # Will be extracted from individual entries
            }
            
            # Standardize race features
            standardized_features = self.standardizer.create_standardized_race_features(raw_race_data)
            
            # Build complete race record
            race_data = {
                'race_id': race_id,
                'track_code': track_code,
                'race_date': race_date,
                'race_number': int(race_number),
                'race_name': self.extract_text(race_element, 'RaceName'),
                'conditions_text': self.extract_text(race_element, 'ConditionText'),
                'post_time': self.extract_text(race_element, 'PostTime'),
                'max_claim_price': self.extract_text(race_element, 'MaximumClaimPrice'),
                'min_claim_price': self.extract_text(race_element, 'MinimumClaimPrice'),
                'source_file': filename,
                'data_source': 'past_performance'
            }
            
            # Add standardized fields
            race_data.update(standardized_features)
            
            return race_data
            
        except Exception as e:
            logger.error(f"Error extracting race data: {e}")
            return None
    
    def extract_entry_data(self, starter_element, race_id: str) -> Optional[Dict]:
        """Extract standardized entry data from XML starter element"""
        try:
            horse_element = starter_element.find('Horse')
            if horse_element is None:
                return None
            
            registration_number = self.extract_text(horse_element, 'RegistrationNumber')
            if not registration_number:
                return None
            
            entry_id = f"{race_id}_{registration_number}"
            
            # Extract raw horse data for standardization
            raw_horse_data = {
                'equipment': self.extract_text(starter_element, 'Equipment/Value'),
                'medication': self.extract_text(starter_element, 'Medication/Value'),
                'weight': self.extract_text(starter_element, 'WeightCarried')
            }
            
            # Standardize horse features
            horse_features = self.standardizer.create_standardized_horse_features(raw_horse_data)
            
            # Calculate age at race
            year_of_birth = self.extract_text(horse_element, 'YearOfBirth')
            race_year = int(race_id.split('_')[1][:4])  # Extract year from race_id
            age_at_race = None
            if year_of_birth:
                try:
                    age_at_race = race_year - int(year_of_birth)
                except ValueError:
                    pass
            
            # Build entry record
            entry_data = {
                'entry_id': entry_id,
                'race_id': race_id,
                'registration_number': registration_number,
                'program_number': self.extract_text(starter_element, 'ProgramNumber'),
                'post_position': self.extract_text(starter_element, 'PostPosition'),
                'age_at_race': age_at_race,
                'claim_price': self.extract_text(starter_element, 'ClaimedPriceUSA'),
                'morning_line_odds': self.extract_text(starter_element, 'Odds'),
                'scratched': self.extract_text(starter_element, 'ScratchIndicator/Value') is not None,
                'trainer_id': self.extract_text(starter_element, 'Trainer/ExternalPartyId'),
                'owner_id': self.extract_text(starter_element, 'Owner/ExternalPartyId'),
                'source_file': '',
                'data_source': 'past_performance'
            }
            
            # Add standardized horse features
            entry_data.update(horse_features)
            
            # Convert numeric fields
            numeric_fields = ['post_position', 'claim_price', 'weight_lbs']
            for field in numeric_fields:
                if entry_data.get(field):
                    try:
                        # Remove non-numeric characters and convert
                        value_str = str(entry_data[field]).replace(',', '').replace('$', '')
                        if '/' in value_str:  # Handle odds like "20/1"
                            if field == 'morning_line_odds':
                                parts = value_str.split('/')
                                entry_data[field] = float(parts[0]) / float(parts[1])
                            else:
                                entry_data[field] = None
                        else:
                            entry_data[field] = float(value_str) if '.' in value_str else int(value_str)
                    except (ValueError, TypeError, ZeroDivisionError):
                        entry_data[field] = None
            
            return entry_data
            
        except Exception as e:
            logger.error(f"Error extracting entry data: {e}")
            return None
    
    def extract_equipment_records(self, entry_data: Dict) -> List[Dict]:
        """Extract individual equipment records for junction table"""
        equipment_records = []
        
        if entry_data.get('equipment_codes'):
            for equipment_code in entry_data['equipment_codes']:
                equipment_record = {
                    'race_id': entry_data['race_id'],
                    'registration_number': entry_data['registration_number'],
                    'equipment_code': equipment_code,
                    'equipment_description': equipment_code.replace('_', ' ').title(),
                    'is_first_time': 'FIRST_TIME' in equipment_code
                }
                equipment_records.append(equipment_record)
        
        return equipment_records
    
    def process_xml_content(self, xml_content: str, filename: str) -> Tuple[int, int]:
        """Process XML content and extract race/entry data"""
        races_count = 0
        entries_count = 0
        
        try:
            root = ET.fromstring(xml_content)
            
            # Extract track code and date from filename
            # Filename pattern: SIMD20230101AQU_USA.xml
            base_filename = os.path.basename(filename).replace('.xml', '')
            if ':' in base_filename:  # Handle zip files
                base_filename = base_filename.split(':')[-1]
                
            if '_' in base_filename:
                parts = base_filename.split('_')
                track_code = parts[1][:3] if len(parts) > 1 else 'UNK'
                date_str = parts[0][4:12] if len(parts[0]) >= 12 else None
            else:
                # For files like SIMD20230101AQU_USA, extract from position
                if len(base_filename) >= 15:
                    date_str = base_filename[4:12]  # positions 4-11 for YYYYMMDD
                    track_code = base_filename[12:15]  # positions 12-14 for track code
                else:
                    track_code = 'UNK'
                    date_str = None
            
            if date_str:
                try:
                    # Convert YYYYMMDD to YYYY-MM-DD
                    race_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                except:
                    race_date = '2023-01-01'  # Default fallback
            else:
                race_date = '2023-01-01'
            
            # Process each race
            for race_element in root.findall('.//Race'):
                # Extract race data
                race_data = self.extract_race_data(race_element, track_code, race_date, filename)
                
                if race_data and race_data['race_id'] not in self.seen_races:
                    self.race_batch.append(race_data)
                    self.seen_races[race_data['race_id']] = True
                    races_count += 1
                    
                    # Process starters for this race
                    for starter_element in race_element.findall('Starters'):
                        entry_data = self.extract_entry_data(starter_element, race_data['race_id'])
                        
                        if entry_data:
                            # Set source file for entry
                            entry_data['source_file'] = filename
                            self.entry_batch.append(entry_data)
                            entries_count += 1
                            
                            # Extract equipment records
                            equipment_records = self.extract_equipment_records(entry_data)
                            self.equipment_batch.extend(equipment_records)
                            
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
                self.stats['races_extracted'] += races
                self.stats['entries_extracted'] += entries
                self.stats['equipment_records'] += len([r for r in self.equipment_batch if r.get('race_id', '').split('_')[0] in filename])
                
                if self.stats['files_processed'] % 100 == 0:
                    logger.info(f"Processed {self.stats['files_processed']} files. "
                              f"Races: {self.stats['races_extracted']}, "
                              f"Entries: {self.stats['entries_extracted']}")
                    
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            with self.lock:
                self.stats['errors'] += 1
    
    def batch_insert_data(self):
        """Insert batched data into database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Insert races
            if self.race_batch:
                race_tuples = []
                for r in list(self.race_batch):
                    race_tuple = (
                        r['race_id'], r['track_code'], r['race_date'], r['race_number'],
                        r.get('race_name'), r.get('conditions_text'),
                        r.get('course_type_code'), r.get('race_type_code'), r.get('track_condition'),
                        r.get('min_age'), r.get('max_age'),
                        r.get('fillies_and_mares', False), r.get('colts_and_geldings', False),
                        r.get('fillies_only', False), r.get('mares_only', False),
                        r.get('colts_only', False), r.get('geldings_only', False),
                        r.get('distance_yards'), r.get('purse_usd'),
                        r.get('max_claim_price'), r.get('min_claim_price'),
                        r.get('class_level'), r.get('purse_category'),
                        r.get('post_time'), r.get('source_file'), r.get('data_source')
                    )
                    race_tuples.append(race_tuple)
                
                cursor.executemany("""
                    INSERT OR IGNORE INTO races_standardized 
                    (race_id, track_code, race_date, race_number, race_name, conditions_text,
                     course_type_code, race_type_code, track_condition,
                     min_age, max_age, fillies_and_mares, colts_and_geldings,
                     fillies_only, mares_only, colts_only, geldings_only,
                     distance_yards, purse_usd, max_claim_price, min_claim_price,
                     class_level, purse_category, post_time, source_file, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, race_tuples)
                
                logger.info(f"Inserted {len(race_tuples)} races")
            
            # Insert entries
            if self.entry_batch:
                entry_tuples = []
                for e in list(self.entry_batch):
                    entry_tuple = (
                        e['entry_id'], e['race_id'], e['registration_number'],
                        e.get('program_number'), e.get('post_position'), e.get('weight_lbs'),
                        e.get('age_at_race'), e.get('has_blinkers', False), e.get('has_lasix', False),
                        e.get('has_tongue_tie', False), e.get('has_nasal_strip', False),
                        e.get('has_shadow_roll', False), e.get('has_cheek_pieces', False),
                        e.get('has_ear_plugs', False), e.get('has_hood', False),
                        e.get('claim_price'), e.get('morning_line_odds'),
                        e.get('trainer_id'), e.get('owner_id'),
                        e.get('scratched', False), e.get('source_file'), e.get('data_source')
                    )
                    entry_tuples.append(entry_tuple)
                
                cursor.executemany("""
                    INSERT OR IGNORE INTO race_entries_standardized
                    (entry_id, race_id, registration_number, program_number, post_position,
                     weight_lbs, age_at_race, has_blinkers, has_lasix, has_tongue_tie,
                     has_nasal_strip, has_shadow_roll, has_cheek_pieces, has_ear_plugs, has_hood,
                     claim_price, morning_line_odds, trainer_id, owner_id, scratched, source_file, data_source)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, entry_tuples)
                
                logger.info(f"Inserted {len(entry_tuples)} entries")
            
            # Insert equipment
            if self.equipment_batch:
                equipment_tuples = []
                for eq in list(self.equipment_batch):
                    equipment_tuple = (
                        eq['race_id'], eq['registration_number'], eq['equipment_code'],
                        eq['equipment_description'], eq['is_first_time']
                    )
                    equipment_tuples.append(equipment_tuple)
                
                cursor.executemany("""
                    INSERT OR IGNORE INTO horse_race_equipment
                    (race_id, registration_number, equipment_code, equipment_description, is_first_time)
                    VALUES (?, ?, ?, ?, ?)
                """, equipment_tuples)
                
                logger.info(f"Inserted {len(equipment_tuples)} equipment records")
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"Database insert error: {e}")
            conn.rollback()
        finally:
            conn.close()
    
    def run_extraction(self, pp_directory: str = "2023 PPs") -> None:
        """Run the full Past Performance extraction process"""
        start_time = time.time()
        logger.info(f"Starting Past Performance extraction from {pp_directory}")
        logger.info(f"Using {self.max_workers} workers with standardization")
        
        # Get all XML files
        xml_files = self.get_xml_files(pp_directory)
        if not xml_files:
            logger.error(f"No XML files found in {pp_directory}")
            return
        
        # Process files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self.process_file, xml_file) for xml_file in xml_files]
            
            for i, future in enumerate(futures):
                try:
                    future.result()
                    
                    # Batch insert when we have enough data
                    if len(self.race_batch) >= self.batch_size:
                        logger.info("Performing batch database insert...")
                        self.batch_insert_data()
                        # Clear batches
                        self.race_batch[:] = []
                        self.entry_batch[:] = []
                        self.equipment_batch[:] = []
                        
                except Exception as e:
                    logger.error(f"Future result error: {e}")
        
        # Final batch insert
        if self.race_batch or self.entry_batch or self.equipment_batch:
            logger.info("Final batch database insert...")
            self.batch_insert_data()
        
        # Final statistics
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("PAST PERFORMANCE EXTRACTION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Files processed: {self.stats['files_processed']}")
        logger.info(f"Races extracted: {self.stats['races_extracted']}")
        logger.info(f"Entries extracted: {self.stats['entries_extracted']}")
        logger.info(f"Equipment records: {self.stats['equipment_records']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Files per second: {self.stats['files_processed'] / duration:.2f}")
        logger.info("=" * 60)

if __name__ == "__main__":
    # Initialize extractor
    extractor = PastPerformanceExtractor(max_workers=45)
    
    # Run extraction
    extractor.run_extraction()