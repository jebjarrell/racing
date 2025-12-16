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
        
        # Check for exact matches first (avoid partial matches)
        words = cleaned.split()
        for code, level in self.race_type_hierarchy.items():
            if code in words or (len(code) > 2 and code in cleaned):
                return {
                    'race_type_code': code,
                    'race_type_description': raw_value.strip(),
                    'class_level': level,
                    'purse_category': self._get_purse_category(level)
                }
        
        # Fallback to keyword matching (order matters - most specific first)
        if any(word in cleaned for word in ['MAIDEN CLAIMING']):
            return {
                'race_type_code': 'MAIDEN',
                'race_type_description': raw_value.strip(),
                'class_level': 1,
                'purse_category': 'MAIDEN'
            }
        elif any(word in cleaned for word in ['MAIDEN', 'MSW']):
            return {
                'race_type_code': 'MAIDEN',
                'race_type_description': raw_value.strip(),
                'class_level': 1,
                'purse_category': 'MAIDEN'
            }
        elif any(word in cleaned for word in ['CLAIMING', 'CLM']):
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
        """Convert distance to yards for standardization"""
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
                # Handle cases where distance is given as 600 meaning 6 furlongs
                if distance >= 100 and distance % 100 == 0:
                    furlongs = distance / 100
                    return int(furlongs * 220)  # Convert furlongs to yards
                else:
                    return int(distance * 220)  # 1 furlong = 220 yards
            elif unit in ['M', 'MILE', 'MILES']:
                return int(distance * 1760)  # 1 mile = 1760 yards
            elif unit in ['Y', 'YARD', 'YARDS']:
                return int(distance)
            else:
                # For raw distance numbers, make reasonable assumptions
                if distance < 20:  # Likely furlongs (most common)
                    return int(distance * 220)
                elif distance >= 100 and distance <= 1000:  # Likely represents furlongs * 100
                    # Values like 600 in XML typically means 6 furlongs, not 600 yards
                    # Convert by dividing by 100 to get furlong count
                    furlongs = distance / 100
                    return int(furlongs * 220)  # Convert furlongs to yards
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

    # =========================================================================
    # SPEED, PACE, AND CLASS CALCULATION METHODS
    # Added for Phase 2: Feature Engineering
    # =========================================================================

    def calculate_speed_figure(
        self,
        final_time: Optional[float],
        distance_yards: int,
        track_variant: float = 0.0,
        par_time: Optional[float] = None
    ) -> Optional[int]:
        """
        Calculate a speed figure from final time.

        Uses a simplified Beyer-style calculation:
        Speed Figure = Base + (Par - Actual) * Scale + Track Variant

        Args:
            final_time: Final time in seconds
            distance_yards: Race distance in yards
            track_variant: Track speed adjustment (positive = fast track)
            par_time: Optional par time for the distance/class

        Returns:
            Speed figure (typically 50-120 range) or None if insufficient data
        """
        if final_time is None or final_time <= 0:
            return None

        if distance_yards <= 0:
            return None

        # Use default par times by distance if not provided
        # These are approximate par times for average claiming races
        default_pars = {
            880: 50.0,    # 4 furlongs
            1100: 63.0,   # 5 furlongs
            1320: 76.0,   # 6 furlongs
            1430: 83.0,   # 6.5 furlongs
            1540: 90.0,   # 7 furlongs
            1650: 97.0,   # 7.5 furlongs
            1760: 103.0,  # 1 mile
            1870: 110.0,  # 1 1/16 miles
            1980: 118.0,  # 1 1/8 miles
        }

        if par_time is None:
            # Find closest par time
            closest_dist = min(default_pars.keys(), key=lambda x: abs(x - distance_yards))
            par_time = default_pars[closest_dist]

            # Adjust for distance difference
            dist_ratio = distance_yards / closest_dist
            par_time = par_time * dist_ratio

        # Base figure (average horse runs par time = 80)
        base_figure = 80

        # Scale: Each second off par = approximately 10 points per mile
        # Adjust scale by distance (shorter = more weight per second)
        scale_factor = 1760 / distance_yards * 10

        # Calculate raw figure
        time_diff = par_time - final_time  # Positive = faster than par
        raw_figure = base_figure + (time_diff * scale_factor) + track_variant

        # Clamp to reasonable range
        return max(0, min(130, int(round(raw_figure))))

    def calculate_pace_figure(
        self,
        fraction_times: List[Optional[float]],
        distance_yards: int
    ) -> Dict[str, any]:
        """
        Calculate pace figures from fractional times.

        Returns early pace (E1, E2) and late pace (LP) figures.

        Args:
            fraction_times: List of fractional times [1/4, 1/2, 3/4, final, etc.]
            distance_yards: Race distance in yards

        Returns:
            Dict with pace figures and style classification
        """
        result = {
            'early_pace_figure': None,
            'late_pace_figure': None,
            'pace_style': 'UNKNOWN',
            'first_call_time': None,
            'second_call_time': None,
        }

        # Filter out None values
        valid_times = [t for t in fraction_times if t is not None and t > 0]

        if len(valid_times) < 2:
            return result

        # Assign times based on position
        if len(valid_times) >= 1:
            result['first_call_time'] = valid_times[0]
        if len(valid_times) >= 2:
            result['second_call_time'] = valid_times[1]

        # Calculate early pace from first two fractions
        # Approximate par for first half-mile: 46 seconds
        first_half_par = 46.0
        first_half_time = valid_times[1] if len(valid_times) >= 2 else valid_times[0] * 2

        early_diff = first_half_par - first_half_time
        result['early_pace_figure'] = int(round(80 + early_diff * 5))

        # Calculate late pace from final fraction
        if len(valid_times) >= 3:
            final_time = valid_times[-1]
            second_to_last = valid_times[-2]
            final_fraction = final_time - second_to_last

            # Par for final quarter: approximately 25 seconds
            final_par = 25.0
            late_diff = final_par - final_fraction
            result['late_pace_figure'] = int(round(80 + late_diff * 5))

        # Classify pace style based on relative figures
        if result['early_pace_figure'] and result['late_pace_figure']:
            early = result['early_pace_figure']
            late = result['late_pace_figure']

            if early >= late + 10:
                result['pace_style'] = 'E'  # Front-runner
            elif early >= late + 3:
                result['pace_style'] = 'EP'  # Early presser
            elif late >= early + 10:
                result['pace_style'] = 'C'  # Closer
            elif late >= early + 3:
                result['pace_style'] = 'S'  # Stalker
            else:
                result['pace_style'] = 'P'  # Presser
        elif result['early_pace_figure']:
            if result['early_pace_figure'] >= 90:
                result['pace_style'] = 'E'
            elif result['early_pace_figure'] >= 80:
                result['pace_style'] = 'EP'
            else:
                result['pace_style'] = 'P'

        return result

    def calculate_class_rating(
        self,
        purse: Optional[float],
        race_type_code: str,
        class_level: int,
        field_quality: float = 80.0
    ) -> float:
        """
        Calculate a class rating incorporating purse, race type, and field.

        Args:
            purse: Purse amount in USD
            race_type_code: Standardized race type code
            class_level: Race class level (1-10)
            field_quality: Average speed figure of field

        Returns:
            Class rating (typically 50-120 range)
        """
        # Base rating from class level
        # Class 1 (maiden) = 60, Class 10 (G1) = 110
        base_rating = 55 + (class_level * 5)

        # Purse adjustment
        # $25k = neutral, each $25k above = +2 points
        purse_adjustment = 0.0
        if purse:
            purse_diff = (purse - 25000) / 25000
            purse_adjustment = purse_diff * 2
            # Cap adjustment
            purse_adjustment = max(-10, min(20, purse_adjustment))

        # Field quality adjustment
        # Average field (80) = neutral
        field_adjustment = (field_quality - 80) * 0.3

        final_rating = base_rating + purse_adjustment + field_adjustment

        return max(40, min(130, final_rating))

    def calculate_earnings_per_start(
        self,
        total_earnings: float,
        total_starts: int
    ) -> float:
        """
        Calculate average earnings per start.

        Args:
            total_earnings: Total career earnings
            total_starts: Total career starts

        Returns:
            Earnings per start
        """
        if total_starts <= 0:
            return 0.0
        return total_earnings / total_starts

    def standardize_odds(
        self,
        odds_value: Optional[str],
        odds_format: str = 'american'
    ) -> Optional[float]:
        """
        Convert odds to decimal format.

        Supports American (+150, -110), fractional (3/1), and decimal (4.0).

        Args:
            odds_value: Odds value as string
            odds_format: Format hint ('american', 'fractional', 'decimal')

        Returns:
            Decimal odds (e.g., 4.0 means $4 return on $1 bet) or None
        """
        if not odds_value:
            return None

        odds_str = str(odds_value).strip()

        try:
            # Try to detect format
            if '/' in odds_str:
                # Fractional: 3/1 -> 4.0
                parts = odds_str.split('/')
                if len(parts) == 2:
                    num = float(parts[0])
                    denom = float(parts[1])
                    if denom > 0:
                        return (num / denom) + 1

            elif odds_str.startswith('+') or odds_str.startswith('-'):
                # American odds
                american = float(odds_str)
                if american > 0:
                    return (american / 100) + 1
                else:
                    return (100 / abs(american)) + 1

            else:
                # Try decimal or fractional without slash
                value = float(odds_str)

                if value < 1:
                    # Probably fractional without denominator (0.5 = 1/2)
                    return value + 1
                elif value < 50:
                    # Likely already decimal
                    return value
                else:
                    # Likely American positive
                    return (value / 100) + 1

        except (ValueError, ZeroDivisionError):
            return None

        return None

    def calculate_implied_probability(self, decimal_odds: float) -> float:
        """
        Convert decimal odds to implied probability.

        Args:
            decimal_odds: Decimal odds (e.g., 4.0)

        Returns:
            Implied probability (0 to 1)
        """
        if decimal_odds <= 0:
            return 0.0
        return 1.0 / decimal_odds

    def normalize_field_probabilities(
        self,
        probabilities: List[float]
    ) -> List[float]:
        """
        Normalize probabilities to sum to 1.0 (softmax-style).

        Used to ensure race probabilities form valid distribution.

        Args:
            probabilities: List of raw probabilities

        Returns:
            Normalized probabilities summing to 1.0
        """
        total = sum(probabilities)
        if total <= 0:
            # Equal probabilities
            n = len(probabilities)
            return [1.0 / n] * n if n > 0 else []

        return [p / total for p in probabilities]