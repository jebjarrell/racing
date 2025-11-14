#!/usr/bin/env python3
"""
High-Performance Horse Data Extraction Script
Extracts horse, trainer, and owner data from 2023 Past Performance XML files
Optimized for 128GB RAM and up to 45 workers
"""

import sqlite3
import xml.etree.ElementTree as ET
import os
import glob
import logging
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Manager, Lock
import time
from datetime import datetime
import traceback
from typing import Dict, List, Tuple, Optional
import zipfile
import tempfile

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HorseExtractor:
    """High-performance horse data extractor"""
    
    def __init__(self, db_path: str = 'racing_data.db', max_workers: int = 45):
        self.db_path = db_path
        self.max_workers = max_workers
        self.manager = Manager()
        self.stats = self.manager.dict({
            'files_processed': 0,
            'horses_extracted': 0,
            'trainers_extracted': 0,
            'owners_extracted': 0,
            'errors': 0,
            'duplicates_skipped': 0
        })
        self.lock = Lock()
        
        # Prepare batch insert lists (shared between processes)
        self.horse_batch = self.manager.list()
        self.trainer_batch = self.manager.list()
        self.owner_batch = self.manager.list()
        
        # Track seen entities to avoid duplicates in memory
        self.seen_horses = self.manager.dict()
        self.seen_trainers = self.manager.dict()
        self.seen_owners = self.manager.dict()
        
        # Batch size for database operations
        self.batch_size = 1000
        
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
                        if file_info.filename.endswith('.xml'):
                            xml_files.append((zip_file, file_info.filename))
            except Exception as e:
                logger.warning(f"Could not read zip file {zip_file}: {e}")
        
        logger.info(f"Found {len(xml_files)} XML files to process")
        return xml_files
        
    def parse_date(self, date_str: str) -> Optional[str]:
        """Parse date string to YYYY-MM-DD format"""
        if not date_str or date_str.strip() == '':
            return None
        try:
            # Handle format: 2001-03-25+00:00
            if '+' in date_str:
                date_str = date_str.split('+')[0]
            # Handle format: 2001-03-25T00:00:00
            if 'T' in date_str:
                date_str = date_str.split('T')[0]
            return date_str
        except:
            return None
            
    def extract_text(self, element, xpath: str) -> Optional[str]:
        """Safely extract text from XML element"""
        try:
            found = element.find(xpath)
            if found is not None and found.text:
                return found.text.strip()
            return None
        except:
            return None
            
    def extract_horse_data(self, horse_element) -> Optional[Dict]:
        """Extract horse data from XML horse element"""
        try:
            registration_number = self.extract_text(horse_element, 'RegistrationNumber')
            if not registration_number:
                return None
                
            horse_data = {
                'registration_number': registration_number,
                'horse_name': self.extract_text(horse_element, 'HorseName'),
                'foaling_date': self.parse_date(self.extract_text(horse_element, 'FoalingDate')),
                'year_of_birth': self.extract_text(horse_element, 'YearOfBirth'),
                'foaling_area': self.extract_text(horse_element, 'FoalingArea'),
                'breed_type': self.extract_text(horse_element, 'BreedType/Value'),
                'color_code': self.extract_text(horse_element, 'Color/Value'),
                'sex_code': self.extract_text(horse_element, 'Sex/Value'),
                'breeder_name': self.extract_text(horse_element, 'BreederName'),
                'sire_registration_number': self.extract_text(horse_element, 'Sire/RegistrationNumber'),
                'dam_registration_number': self.extract_text(horse_element, 'Dam/RegistrationNumber'),
            }
            
            # Convert year_of_birth to int
            if horse_data['year_of_birth']:
                try:
                    horse_data['year_of_birth'] = int(horse_data['year_of_birth'])
                except:
                    horse_data['year_of_birth'] = None
            
            return horse_data
            
        except Exception as e:
            logger.error(f"Error extracting horse data: {e}")
            return None
            
    def extract_trainer_data(self, trainer_element) -> Optional[Dict]:
        """Extract trainer data from XML trainer element"""
        try:
            external_party_id = self.extract_text(trainer_element, 'ExternalPartyId')
            if not external_party_id:
                return None
                
            return {
                'external_party_id': external_party_id,
                'first_name': self.extract_text(trainer_element, 'FirstName'),
                'middle_name': self.extract_text(trainer_element, 'MiddleName'),
                'last_name': self.extract_text(trainer_element, 'LastName'),
                'type_source': self.extract_text(trainer_element, 'TypeSource'),
            }
            
        except Exception as e:
            logger.error(f"Error extracting trainer data: {e}")
            return None
            
    def extract_owner_data(self, owner_element) -> Optional[Dict]:
        """Extract owner data from XML owner element"""
        try:
            external_party_id = self.extract_text(owner_element, 'ExternalPartyId')
            if not external_party_id:
                return None
                
            return {
                'external_party_id': external_party_id,
                'first_name': self.extract_text(owner_element, 'FirstName'),
                'middle_name': self.extract_text(owner_element, 'MiddleName'),
                'last_name': self.extract_text(owner_element, 'LastName'),
                'type_source': self.extract_text(owner_element, 'TypeSource'),
            }
            
        except Exception as e:
            logger.error(f"Error extracting owner data: {e}")
            return None
            
    def process_xml_content(self, xml_content: str, filename: str) -> Tuple[int, int, int]:
        """Process XML content and extract horse/trainer/owner data"""
        horses_count = 0
        trainers_count = 0
        owners_count = 0
        
        try:
            root = ET.fromstring(xml_content)
            
            # Find all race elements: EntryRaceCard/Race
            for race in root.findall('.//Race'):
                # Find all Starters elements in this race
                for starter in race.findall('Starters'):
                    # Extract horse data from Horse element
                    horse_element = starter.find('Horse')
                    if horse_element is not None:
                        horse_data = self.extract_horse_data(horse_element)
                        if horse_data and horse_data['registration_number'] not in self.seen_horses:
                            self.horse_batch.append(horse_data)
                            self.seen_horses[horse_data['registration_number']] = True
                            horses_count += 1
                    
                    # Extract trainer data from Trainer element (sibling of Horse)
                    trainer_element = starter.find('Trainer')
                    if trainer_element is not None:
                        trainer_data = self.extract_trainer_data(trainer_element)
                        if trainer_data and trainer_data['external_party_id'] not in self.seen_trainers:
                            self.trainer_batch.append(trainer_data)
                            self.seen_trainers[trainer_data['external_party_id']] = True
                            trainers_count += 1
                    
                    # Extract owner data from Owner element (sibling of Horse)
                    owner_element = starter.find('Owner')
                    if owner_element is not None:
                        owner_data = self.extract_owner_data(owner_element)
                        if owner_data and owner_data['external_party_id'] not in self.seen_owners:
                            self.owner_batch.append(owner_data)
                            self.seen_owners[owner_data['external_party_id']] = True
                            owners_count += 1
                            
        except ET.ParseError as e:
            logger.error(f"XML parse error in {filename}: {e}")
            self.stats['errors'] += 1
        except Exception as e:
            logger.error(f"Unexpected error processing {filename}: {e}")
            logger.error(traceback.format_exc())
            self.stats['errors'] += 1
            
        return horses_count, trainers_count, owners_count
        
    def process_file(self, file_path) -> None:
        """Process a single XML file (can be direct file or zip entry)"""
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
            
            horses, trainers, owners = self.process_xml_content(xml_content, filename)
            
            with self.lock:
                self.stats['files_processed'] += 1
                self.stats['horses_extracted'] += horses
                self.stats['trainers_extracted'] += trainers
                self.stats['owners_extracted'] += owners
                
                if self.stats['files_processed'] % 100 == 0:
                    logger.info(f"Processed {self.stats['files_processed']} files. "
                              f"Horses: {self.stats['horses_extracted']}, "
                              f"Trainers: {self.stats['trainers_extracted']}, "
                              f"Owners: {self.stats['owners_extracted']}")
                    
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
            with self.lock:
                self.stats['errors'] += 1
                
    def batch_insert_data(self):
        """Insert batched data into database"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            # Insert horses
            if self.horse_batch:
                horse_tuples = [
                    (h['registration_number'], h['horse_name'], h['foaling_date'], 
                     h['year_of_birth'], h['foaling_area'], h['breed_type'],
                     h['color_code'], h['sex_code'], h['breeder_name'],
                     h['sire_registration_number'], h['dam_registration_number'])
                    for h in list(self.horse_batch)
                ]
                
                cursor.executemany("""
                    INSERT OR IGNORE INTO horses_master 
                    (registration_number, horse_name, foaling_date, year_of_birth, 
                     foaling_area, breed_type, color_code, sex_code, breeder_name,
                     sire_registration_number, dam_registration_number)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, horse_tuples)
                
                logger.info(f"Inserted {len(horse_tuples)} horses")
                
            # Insert trainers
            if self.trainer_batch:
                trainer_tuples = [
                    (t['external_party_id'], t['first_name'], t['middle_name'], 
                     t['last_name'], t['type_source'])
                    for t in list(self.trainer_batch)
                ]
                
                cursor.executemany("""
                    INSERT OR IGNORE INTO trainers 
                    (external_party_id, first_name, middle_name, last_name, type_source)
                    VALUES (?, ?, ?, ?, ?)
                """, trainer_tuples)
                
                logger.info(f"Inserted {len(trainer_tuples)} trainers")
                
            # Insert owners
            if self.owner_batch:
                owner_tuples = [
                    (o['external_party_id'], o['first_name'], o['middle_name'], 
                     o['last_name'], o['type_source'])
                    for o in list(self.owner_batch)
                ]
                
                cursor.executemany("""
                    INSERT OR IGNORE INTO owners 
                    (external_party_id, first_name, middle_name, last_name, type_source)
                    VALUES (?, ?, ?, ?, ?)
                """, owner_tuples)
                
                logger.info(f"Inserted {len(owner_tuples)} owners")
                
            conn.commit()
            
        except Exception as e:
            logger.error(f"Database insert error: {e}")
            conn.rollback()
        finally:
            conn.close()
            
    def run_extraction(self, pp_directory: str = "2023 PPs") -> None:
        """Run the full extraction process"""
        start_time = time.time()
        logger.info(f"Starting horse data extraction from {pp_directory}")
        logger.info(f"Using {self.max_workers} workers with high-memory optimization")
        
        # Get all XML files
        xml_files = self.get_xml_files(pp_directory)
        if not xml_files:
            logger.error(f"No XML files found in {pp_directory}")
            return
            
        # Process files in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all files for processing
            futures = [executor.submit(self.process_file, xml_file) for xml_file in xml_files]
            
            # Monitor progress
            for i, future in enumerate(futures):
                try:
                    future.result()
                    
                    # Batch insert when we have enough data
                    if len(self.horse_batch) >= self.batch_size:
                        logger.info("Performing batch database insert...")
                        self.batch_insert_data()
                        # Clear batches
                        self.horse_batch[:] = []
                        self.trainer_batch[:] = []
                        self.owner_batch[:] = []
                        
                except Exception as e:
                    logger.error(f"Future result error: {e}")
                    
        # Final batch insert for remaining data
        if self.horse_batch or self.trainer_batch or self.owner_batch:
            logger.info("Final batch database insert...")
            self.batch_insert_data()
            
        # Final statistics
        end_time = time.time()
        duration = end_time - start_time
        
        logger.info("=" * 60)
        logger.info("EXTRACTION COMPLETE")
        logger.info("=" * 60)
        logger.info(f"Files processed: {self.stats['files_processed']}")
        logger.info(f"Horses extracted: {self.stats['horses_extracted']}")
        logger.info(f"Trainers extracted: {self.stats['trainers_extracted']}")
        logger.info(f"Owners extracted: {self.stats['owners_extracted']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Files per second: {self.stats['files_processed'] / duration:.2f}")
        logger.info("=" * 60)

if __name__ == "__main__":
    # Initialize extractor with high-performance settings
    extractor = HorseExtractor(max_workers=45)
    
    # Run extraction
    extractor.run_extraction()