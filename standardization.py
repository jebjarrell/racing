#!/usr/bin/env python3
"""
Data Standardization Functions for Racing Pipeline
Normalizes categorical and text fields for consistent feature engineering
"""

import re
from typing import List, Dict, Optional, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

class RacingDataStandardizer:
    """Standardizes racing data fields for consistent feature engineering"""
    
    def __init__(self):
        # Course type mappings
        self.dirt_variations = {
            'D', 'DIRT', 'FAST', 'SLOPPY', 'MUDDY', 'GOOD', 'SEALED', 'FROZEN'
        }
        
        self.turf_variations = {
            'T', 'TURF', 'FIRM', 'GOOD TO FIRM', 'YIELDING', 'SOFT', 'HEAVY',
            'GRASS', 'LAWN'
        }
        
        self.synthetic_variations = {
            'S', 'SYNTH', 'SYNTHETIC', 'TAPETA', 'POLYTRACK', 'FIBRESAND',
            'CUSHION', 'PRO-RIDE'
        }
        
        # Race type hierarchies (higher number = higher class)
        self.race_type_hierarchy = {
            # Stakes races (highest class)
            'G1': 10, 'G2': 9, 'G3': 8, 'GR1': 10, 'GR2': 9, 'GR3': 8,
            'L': 7, 'LR': 7, 'LISTED': 7,
            'STK': 6, 'STAKES': 6, 'BT': 6,
            
            # Allowance races
            'ALW': 5, 'ALLOWANCE': 5, 'AOC': 5, 'N1X': 4, 'N2X': 3,
            
            # Claiming races  
            'CLM': 2, 'CLAIMING': 2, 'CL': 2,
            
            # Maiden races (lowest class)
            'MSW': 1, 'MAIDEN': 1, 'MCL': 1, 'MAIDEN CLAIMING': 1,
            'MSP': 1, 'MAIDEN SPECIAL WEIGHT': 1
        }
        
        # Equipment standardization
        self.equipment_mappings = {
            'B': 'BLINKERS', 'BLINKERS': 'BLINKERS',
            'BF': 'BLINKERS_FIRST_TIME', 'BL': 'BLINKERS_LASIX',
            'L': 'LASIX', 'L1': 'LASIX_FIRST_TIME', 'L2': 'LASIX_SECOND_TIME',
            'LASIX': 'LASIX', 'SALIX': 'LASIX',
            'T': 'TONGUE_TIE', 'TT': 'TONGUE_TIE',
            'N': 'NASAL_STRIP', 'NS': 'NASAL_STRIP',
            'S': 'SHADOW_ROLL', 'SR': 'SHADOW_ROLL',
            'E': 'EAR_PLUGS', 'EP': 'EAR_PLUGS',
            'H': 'HOOD', 'HOOD': 'HOOD',
            'C': 'CHEEK_PIECES', 'CP': 'CHEEK_PIECES'
        }
        
        # Track condition mappings
        self.track_conditions = {
            'FAST': 'FAST', 'FT': 'FAST', 'F': 'FAST',
            'GOOD': 'GOOD', 'GD': 'GOOD', 'G': 'GOOD',
            'SLOPPY': 'SLOPPY', 'SL': 'SLOPPY', 'SLPY': 'SLOPPY',
            'MUDDY': 'MUDDY', 'MY': 'MUDDY', 'MD': 'MUDDY',
            'WF': 'WET_FAST', 'WET FAST': 'WET_FAST',
            'FIRM': 'FIRM', 'FM': 'FIRM',
            'YIELDING': 'YIELDING', 'YL': 'YIELDING', 'Y': 'YIELDING',
            'SOFT': 'SOFT', 'SF': 'SOFT',
            'HEAVY': 'HEAVY', 'HV': 'HEAVY'
        }
    
    def standardize_course_type(self, raw_value: Optional[str]) -> str:
        """Normalize course type to standard categories"""
        if not raw_value or raw_value.strip() == '':
            return 'UNKNOWN'
        
        cleaned = raw_value.strip().upper()
        
        if cleaned in self.dirt_variations:
            return 'DIRT'
        elif cleaned in self.turf_variations:
            return 'TURF' 
        elif cleaned in self.synthetic_variations:
            return 'SYNTHETIC'
        else:
            return 'UNKNOWN'
    
    def standardize_race_type(self, raw_value: Optional[str]) -> Dict[str, any]:
        """Parse and standardize race type with classification"""
        if not raw_value or raw_value.strip() == '':
            return {
                'race_type_code': 'UNKNOWN',
                'race_type_description': 'Unknown',
                'class_level': 0,
                'purse_category': 'UNKNOWN'
            }
        
        cleaned = raw_value.strip().upper()

        # Handle compound types first (most specific first)
        # Must check these before individual keyword matching
        if 'MAIDEN CLAIMING' in cleaned or 'MAIDEN CLM' in cleaned:
            return {
                'race_type_code': 'CLAIMING',  # Use CLAIMING code per standardization
                'race_type_description': raw_value.strip(),
                'class_level': 1,  # Maiden races are lowest class
                'purse_category': 'MAIDEN'
            }

        # Check for exact matches in race type hierarchy
        words = cleaned.split()
        for code, level in self.race_type_hierarchy.items():
            # Only match if code appears as a complete word, not as substring
            if code in words:
                return {
                    'race_type_code': code,
                    'race_type_description': raw_value.strip(),
                    'class_level': level,
                    'purse_category': self._get_purse_category(level)
                }

        # Fallback to keyword matching (order matters - most specific first)
        if 'MAIDEN' in cleaned or 'MSW' in cleaned:
            return {
                'race_type_code': 'MAIDEN',
                'race_type_description': raw_value.strip(),
                'class_level': 1,
                'purse_category': 'MAIDEN'
            }
        elif 'CLAIMING' in cleaned or 'CLM' in cleaned:
            return {
                'race_type_code': 'CLAIMING',
                'race_type_description': raw_value.strip(),
                'class_level': 2,
                'purse_category': 'CLAIMING'
            }
        elif any(word in cleaned for word in ['ALLOWANCE', 'ALW']):
            return {
                'race_type_code': 'ALLOWANCE',
                'race_type_description': raw_value.strip(),
                'class_level': 5,
                'purse_category': 'ALLOWANCE'
            }
        elif any(word in cleaned for word in ['STAKES', 'STK']):
            return {
                'race_type_code': 'STAKES',
                'race_type_description': raw_value.strip(),
                'class_level': 6,
                'purse_category': 'STAKES'
            }
        else:
            return {
                'race_type_code': 'OTHER',
                'race_type_description': raw_value.strip(),
                'class_level': 3,
                'purse_category': 'OTHER'
            }
    
    def _get_purse_category(self, class_level: int) -> str:
        """Map class level to purse category"""
        if class_level >= 8:
            return 'GRADED_STAKES'
        elif class_level >= 6:
            return 'STAKES'
        elif class_level >= 4:
            return 'ALLOWANCE'
        elif class_level >= 2:
            return 'CLAIMING'
        elif class_level == 1:
            return 'MAIDEN'
        else:
            return 'UNKNOWN'
    
    def parse_age_restrictions(self, raw_value: Optional[str]) -> Dict[str, Optional[int]]:
        """Parse age restrictions into min/max ranges"""
        if not raw_value or raw_value.strip() == '':
            return {'min_age': None, 'max_age': None}
        
        cleaned = raw_value.strip().upper()
        
        # Common patterns
        patterns = [
            (r'(\d+)YO', r'\1', r'\1'),  # "3YO" -> min=3, max=3
            (r'(\d+)U', r'\1', None),    # "4U" -> min=4, max=None (4 and up)
            (r'(\d+)\+', r'\1', None),   # "3+" -> min=3, max=None
            (r'(\d+)-(\d+)', r'\1', r'\2'), # "3-5" -> min=3, max=5
            (r'(\d+)&UP', r'\1', None),  # "4&UP" -> min=4, max=None
            (r'(\d+) AND UP', r'\1', None), # "3 AND UP" -> min=3, max=None
            (r'(\d+) YEARS OLD AND UP', r'\1', None)
        ]
        
        for pattern, min_group, max_group in patterns:
            match = re.search(pattern, cleaned)
            if match:
                try:
                    min_age = int(match.group(1)) if min_group else None
                    max_age = int(match.group(2)) if max_group and len(match.groups()) > 1 else None
                    if max_group == r'\1':  # Same as min_age
                        max_age = min_age
                    return {'min_age': min_age, 'max_age': max_age}
                except (ValueError, IndexError):
                    continue
        
        return {'min_age': None, 'max_age': None}
    
    def standardize_sex_restrictions(self, raw_value: Optional[str]) -> Dict[str, bool]:
        """Parse sex restrictions into boolean flags"""
        if not raw_value or raw_value.strip() == '':
            return {
                'fillies_and_mares': False,
                'colts_and_geldings': False,
                'fillies_only': False,
                'mares_only': False,
                'colts_only': False,
                'geldings_only': False
            }
        
        cleaned = raw_value.strip().upper()
        
        # Initialize flags
        flags = {
            'fillies_and_mares': False,
            'colts_and_geldings': False,
            'fillies_only': False,
            'mares_only': False,
            'colts_only': False,
            'geldings_only': False
        }
        
        # Check for specific restrictions
        if 'FILLIES AND MARES' in cleaned or 'F&M' in cleaned:
            flags['fillies_and_mares'] = True
        elif 'FILLIES' in cleaned and 'MARES' not in cleaned:
            flags['fillies_only'] = True
        elif 'MARES' in cleaned and 'FILLIES' not in cleaned:
            flags['mares_only'] = True
        elif 'COLTS AND GELDINGS' in cleaned:
            flags['colts_and_geldings'] = True
        elif 'COLTS' in cleaned and 'GELDINGS' not in cleaned:
            flags['colts_only'] = True
        elif 'GELDINGS' in cleaned and 'COLTS' not in cleaned:
            flags['geldings_only'] = True
        
        return flags
    
    def standardize_equipment(self, equipment_string: Optional[str]) -> List[str]:
        """Parse equipment combinations into standardized codes"""
        if not equipment_string or equipment_string.strip() == '':
            return []
        
        equipment_list = []
        # Split on common delimiters
        items = re.split(r'[,;/\s]+', equipment_string.strip().upper())
        
        for item in items:
            item = item.strip()
            if item and item in self.equipment_mappings:
                standardized = self.equipment_mappings[item]
                if standardized not in equipment_list:
                    equipment_list.append(standardized)
            elif item:  # Unknown equipment, keep as-is
                equipment_list.append(item)
        
        return equipment_list
    
    def parse_weight(self, weight_value: Optional[str]) -> Optional[int]:
        """Extract numeric weight in pounds"""
        if not weight_value:
            return None
        
        # Extract numeric value
        weight_str = str(weight_value).strip()
        match = re.search(r'(\d+)', weight_str)
        
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                return None
        
        return None
    
    def standardize_track_condition(self, raw_value: Optional[str]) -> str:
        """Normalize track condition"""
        if not raw_value or raw_value.strip() == '':
            return 'UNKNOWN'
        
        cleaned = raw_value.strip().upper()
        
        return self.track_conditions.get(cleaned, 'OTHER')
    
    def parse_distance(self, distance_value: Optional[str], unit: Optional[str] = None) -> Optional[int]:
        """Convert distance to yards for standardization

        Equibase XML uses "hundredths" encoding for distances:
        - Furlongs: 600 = 6.00F, 550 = 5.50F, 1430 = 14.30F (divide by 100)
        - Miles: 2400 = 1.5M (divide by 1600 to get miles, as 1 mile = 1600 in their encoding)
        """
        if not distance_value:
            return None

        try:
            distance = float(str(distance_value).strip())

            # Determine unit from context if not provided
            if not unit:
                if distance < 20:  # Likely furlongs
                    unit = 'F'
                elif distance > 1000:  # Likely yards
                    unit = 'Y'
                else:  # Likely miles
                    unit = 'M'

            unit = str(unit).upper() if unit else 'F'

            # Convert to yards
            if unit in ['F', 'FURLONG', 'FURLONGS']:
                # Equibase encodes furlongs in "hundredths" format
                # E.g., 600 = 6.00 furlongs, 550 = 5.50 furlongs
                if distance >= 100:
                    # Divide by 100 to get actual furlongs
                    furlongs = distance / 100
                    return int(furlongs * 220)  # Convert furlongs to yards
                else:
                    # Small values (< 100) are already in furlongs
                    return int(distance * 220)  # 1 furlong = 220 yards

            elif unit in ['M', 'MILE', 'MILES']:
                # Equibase encodes miles in a special format where 1 mile = 1600
                # E.g., 2400 = 1.5 miles (2400/1600 = 1.5)
                if distance >= 100:
                    # Divide by 1600 to get actual miles
                    miles = distance / 1600
                    return int(miles * 1760)  # Convert miles to yards
                else:
                    # Small values (< 100) are already in miles
                    return int(distance * 1760)  # 1 mile = 1760 yards

            elif unit in ['Y', 'YARD', 'YARDS']:
                return int(distance)
            else:
                # For raw distance numbers without explicit unit, make reasonable assumptions
                if distance < 20:  # Likely furlongs (most common)
                    return int(distance * 220)
                elif distance >= 100 and distance <= 1000:  # Likely represents furlongs in hundredths
                    furlongs = distance / 100
                    return int(furlongs * 220)
                elif distance > 1000:  # Likely already in yards or feet
                    if distance > 5000:  # Definitely feet
                        return int(distance / 3)  # Convert feet to yards
                    else:
                        return int(distance)  # Assume yards
                else:  # Could be furlongs
                    return int(distance * 220)

        except (ValueError, TypeError):
            return None
    
    def create_standardized_race_features(self, race_data: Dict) -> Dict:
        """Create complete standardized race feature set"""
        
        features = {}
        
        # Course and surface
        features.update({
            'course_type_code': self.standardize_course_type(race_data.get('course_type')),
            'track_condition': self.standardize_track_condition(race_data.get('track_condition'))
        })
        
        # Race type and classification
        race_type_data = self.standardize_race_type(race_data.get('race_type'))
        features.update(race_type_data)
        
        # Age restrictions
        age_data = self.parse_age_restrictions(race_data.get('age_restrictions'))
        features.update(age_data)
        
        # Sex restrictions
        sex_data = self.standardize_sex_restrictions(race_data.get('sex_restrictions'))
        features.update(sex_data)
        
        # Distance standardization
        features['distance_yards'] = self.parse_distance(
            race_data.get('distance'),
            race_data.get('distance_unit')
        )
        
        # Purse standardization
        try:
            purse_str = str(race_data.get('purse', '')).replace(',', '').replace('$', '')
            features['purse_usd'] = float(purse_str) if purse_str else None
        except (ValueError, TypeError):
            features['purse_usd'] = None
        
        return features
    
    def create_standardized_horse_features(self, horse_data: Dict) -> Dict:
        """Create standardized horse-specific features"""
        
        features = {}
        
        # Equipment standardization
        equipment_list = self.standardize_equipment(horse_data.get('equipment'))
        features['equipment_codes'] = equipment_list
        
        # Create boolean flags for common equipment
        common_equipment = ['BLINKERS', 'LASIX', 'TONGUE_TIE', 'NASAL_STRIP']
        for equip in common_equipment:
            features[f'has_{equip.lower()}'] = equip in equipment_list
        
        # Weight standardization
        features['weight_lbs'] = self.parse_weight(horse_data.get('weight'))
        
        # Medication flags
        medication_list = self.standardize_equipment(horse_data.get('medication'))
        features['medication_codes'] = medication_list
        features['has_lasix'] = 'LASIX' in medication_list or 'LASIX' in equipment_list
        
        return features