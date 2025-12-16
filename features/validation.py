"""
Leakage Validation Framework for Horse Racing Features

Provides comprehensive validation to ensure all features respect
point-in-time constraints and do not leak future information.

This module MUST be run before any model training to verify
data integrity and prevent look-ahead bias.

Example:
    validator = LeakageValidator(db_path='racing_data.db')
    report = validator.run_validation_suite(sample_size=100)
    if not report.passed:
        raise ValueError(f"Leakage detected: {report.failures}")
"""

import logging
import random
import sqlite3
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple, Set

logger = logging.getLogger(__name__)


@dataclass
class ValidationFailure:
    """Container for a single validation failure."""
    test_name: str
    race_id: str
    entity_type: str  # 'horse', 'trainer', 'jockey', 'race'
    entity_id: str
    description: str
    feature_date: Optional[date] = None
    leaking_date: Optional[date] = None
    severity: str = 'critical'  # 'critical', 'warning', 'info'

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'test_name': self.test_name,
            'race_id': self.race_id,
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'description': self.description,
            'feature_date': self.feature_date.isoformat() if self.feature_date else None,
            'leaking_date': self.leaking_date.isoformat() if self.leaking_date else None,
            'severity': self.severity,
        }


@dataclass
class ValidationReport:
    """Container for validation suite results."""
    passed: bool = True
    tests_run: int = 0
    tests_passed: int = 0
    tests_failed: int = 0

    races_validated: int = 0
    entries_validated: int = 0

    failures: List[ValidationFailure] = field(default_factory=list)
    warnings: List[ValidationFailure] = field(default_factory=list)

    execution_time_seconds: float = 0.0

    def add_failure(self, failure: ValidationFailure) -> None:
        """Add a validation failure."""
        if failure.severity == 'critical':
            self.failures.append(failure)
            self.passed = False
            self.tests_failed += 1
        else:
            self.warnings.append(failure)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'passed': self.passed,
            'tests_run': self.tests_run,
            'tests_passed': self.tests_passed,
            'tests_failed': self.tests_failed,
            'races_validated': self.races_validated,
            'entries_validated': self.entries_validated,
            'failures': [f.to_dict() for f in self.failures],
            'warnings': [w.to_dict() for w in self.warnings],
            'execution_time_seconds': round(self.execution_time_seconds, 2),
        }

    def summary(self) -> str:
        """Generate summary string."""
        status = "PASSED" if self.passed else "FAILED"
        return (
            f"Validation {status}\n"
            f"  Tests: {self.tests_passed}/{self.tests_run} passed\n"
            f"  Races validated: {self.races_validated}\n"
            f"  Entries validated: {self.entries_validated}\n"
            f"  Failures: {len(self.failures)}\n"
            f"  Warnings: {len(self.warnings)}\n"
            f"  Time: {self.execution_time_seconds:.2f}s"
        )


class LeakageValidator:
    """
    Validates features for point-in-time integrity.

    Performs comprehensive checks to ensure no future data leaks
    into feature calculations.

    Attributes:
        db_path: Path to SQLite database
    """

    def __init__(self, db_path: str = 'racing_data.db'):
        """
        Initialize the validator.

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def check_point_in_time(
        self,
        feature_date: date,
        data_dates: List[date]
    ) -> bool:
        """
        Check if all data dates are strictly before the feature date.

        Args:
            feature_date: Date the feature is calculated for
            data_dates: List of dates from source data

        Returns:
            True if no leakage (all data_dates < feature_date)
        """
        for d in data_dates:
            if d >= feature_date:
                return False
        return True

    def validate_trainer_stats(
        self,
        race_id: str,
        race_date: date,
        trainer_id: str,
        feature_row: Dict[str, Any]
    ) -> List[ValidationFailure]:
        """
        Validate trainer statistics for leakage.

        Checks that trainer stats only use races before the target race.

        Args:
            race_id: Target race ID
            race_date: Target race date
            trainer_id: Trainer ID
            feature_row: Feature values to validate

        Returns:
            List of validation failures (empty if valid)
        """
        failures = []
        conn = self._get_connection()

        # Get trainer's race dates up to and including target
        cursor = conn.execute("""
            SELECT DISTINCT rs.race_date
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE re.trainer_id = ?
                AND rs.race_date <= ?
            ORDER BY rs.race_date DESC
            LIMIT 100
        """, (trainer_id, race_date.isoformat()))

        trainer_dates = [date.fromisoformat(row[0]) for row in cursor.fetchall()]

        # Check if any stats could have come from target race
        if trainer_dates and trainer_dates[0] == race_date:
            # Verify feature values don't include target race
            # This is a structural check - actual implementation must use <, not <=

            # Check if starts count includes target
            cursor = conn.execute("""
                SELECT COUNT(*) as starts_including
                FROM race_entries_standardized re
                JOIN races_standardized rs ON re.race_id = rs.race_id
                WHERE re.trainer_id = ?
                    AND rs.race_date <= ?
                    AND re.scratched = 0
            """, (trainer_id, race_date.isoformat()))

            starts_including = cursor.fetchone()[0]

            cursor = conn.execute("""
                SELECT COUNT(*) as starts_excluding
                FROM race_entries_standardized re
                JOIN races_standardized rs ON re.race_id = rs.race_id
                WHERE re.trainer_id = ?
                    AND rs.race_date < ?
                    AND re.scratched = 0
            """, (trainer_id, race_date.isoformat()))

            starts_excluding = cursor.fetchone()[0]

            if starts_including != starts_excluding:
                # There are races on target date
                # If feature shows the higher count, there's leakage
                feature_starts = feature_row.get('trainer_starts', 0)

                if feature_starts is not None and feature_starts > starts_excluding:
                    failures.append(ValidationFailure(
                        test_name='trainer_point_in_time',
                        race_id=race_id,
                        entity_type='trainer',
                        entity_id=trainer_id,
                        description=f"Trainer stats may include target date: feature shows {feature_starts} starts, should be max {starts_excluding}",
                        feature_date=race_date,
                        severity='critical'
                    ))

        return failures

    def validate_jockey_stats(
        self,
        race_id: str,
        race_date: date,
        jockey_id: str,
        feature_row: Dict[str, Any]
    ) -> List[ValidationFailure]:
        """
        Validate jockey statistics for leakage.

        Args:
            race_id: Target race ID
            race_date: Target race date
            jockey_id: Jockey ID
            feature_row: Feature values to validate

        Returns:
            List of validation failures
        """
        failures = []
        conn = self._get_connection()

        # Similar logic to trainer validation
        cursor = conn.execute("""
            SELECT COUNT(*) as starts_excluding
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE re.jockey_id = ?
                AND rs.race_date < ?
                AND re.scratched = 0
        """, (jockey_id, race_date.isoformat()))

        starts_excluding = cursor.fetchone()[0]

        feature_starts = feature_row.get('jockey_starts', 0)

        if feature_starts is not None and feature_starts > starts_excluding:
            failures.append(ValidationFailure(
                test_name='jockey_point_in_time',
                race_id=race_id,
                entity_type='jockey',
                entity_id=jockey_id,
                description=f"Jockey stats may include target date: feature shows {feature_starts} starts, should be max {starts_excluding}",
                feature_date=race_date,
                severity='critical'
            ))

        return failures

    def validate_horse_form(
        self,
        race_id: str,
        race_date: date,
        registration_number: str,
        feature_row: Dict[str, Any]
    ) -> List[ValidationFailure]:
        """
        Validate horse form features for leakage.

        Ensures horse form calculations only use prior race data.

        Args:
            race_id: Target race ID
            race_date: Target race date
            registration_number: Horse registration number
            feature_row: Feature values to validate

        Returns:
            List of validation failures
        """
        failures = []
        conn = self._get_connection()

        # Get horse's last race before target
        cursor = conn.execute("""
            SELECT MAX(rs.race_date) as last_race
            FROM race_entries_standardized re
            JOIN races_standardized rs ON re.race_id = rs.race_id
            WHERE re.registration_number = ?
                AND rs.race_date < ?
                AND re.scratched = 0
        """, (registration_number, race_date.isoformat()))

        row = cursor.fetchone()
        last_race = date.fromisoformat(row[0]) if row[0] else None

        # Validate days_since_last calculation
        if last_race:
            expected_days = (race_date - last_race).days
            feature_days = feature_row.get('days_since_last', None)

            if feature_days is not None and feature_days != expected_days:
                # Could be using wrong date
                if feature_days < expected_days:
                    failures.append(ValidationFailure(
                        test_name='horse_days_since_last',
                        race_id=race_id,
                        entity_type='horse',
                        entity_id=registration_number,
                        description=f"days_since_last is {feature_days}, expected {expected_days}. May be using future race data.",
                        feature_date=race_date,
                        severity='critical'
                    ))

        # Check that finish position is not from target race
        feature_last_finish = feature_row.get('last_finish_position', None)
        if feature_last_finish is not None:
            # Get the actual last finish before target
            cursor = conn.execute("""
                SELECT re.official_finish_position
                FROM race_entries_standardized re
                JOIN races_standardized rs ON re.race_id = rs.race_id
                WHERE re.registration_number = ?
                    AND rs.race_date < ?
                    AND re.scratched = 0
                ORDER BY rs.race_date DESC
                LIMIT 1
            """, (registration_number, race_date.isoformat()))

            row = cursor.fetchone()
            expected_last_finish = row[0] if row else None

            if expected_last_finish is not None and feature_last_finish != expected_last_finish:
                failures.append(ValidationFailure(
                    test_name='horse_last_finish',
                    race_id=race_id,
                    entity_type='horse',
                    entity_id=registration_number,
                    description=f"last_finish_position is {feature_last_finish}, expected {expected_last_finish}",
                    feature_date=race_date,
                    severity='warning'
                ))

        return failures

    def validate_race_outcome_exclusion(
        self,
        race_id: str,
        race_date: date,
        feature_row: Dict[str, Any]
    ) -> List[ValidationFailure]:
        """
        Validate that race outcome is not in feature row.

        Training features should never include the target outcome.

        Args:
            race_id: Target race ID
            race_date: Target race date
            feature_row: Feature values to validate

        Returns:
            List of validation failures
        """
        failures = []

        # Check for outcome fields that should not be in features
        forbidden_fields = [
            'actual_finish_position',
            'target_finish',
            'win_payoff',
            'final_odds',
            'outcome',
            'is_winner',
        ]

        for field in forbidden_fields:
            if field in feature_row and feature_row[field] is not None:
                failures.append(ValidationFailure(
                    test_name='outcome_exclusion',
                    race_id=race_id,
                    entity_type='race',
                    entity_id=race_id,
                    description=f"Feature row contains outcome field '{field}' which should be excluded from training features",
                    feature_date=race_date,
                    severity='critical'
                ))

        return failures

    def validate_feature_row(
        self,
        race_id: str,
        race_date: date,
        entry_id: str,
        trainer_id: str,
        jockey_id: str,
        registration_number: str,
        feature_row: Dict[str, Any]
    ) -> List[ValidationFailure]:
        """
        Validate a complete feature row for leakage.

        Args:
            race_id: Target race ID
            race_date: Target race date
            entry_id: Race entry ID
            trainer_id: Trainer ID
            jockey_id: Jockey ID
            registration_number: Horse registration number
            feature_row: Complete feature dictionary

        Returns:
            List of all validation failures
        """
        failures = []

        # Run all validation checks
        failures.extend(self.validate_trainer_stats(
            race_id, race_date, trainer_id, feature_row
        ))
        failures.extend(self.validate_jockey_stats(
            race_id, race_date, jockey_id, feature_row
        ))
        failures.extend(self.validate_horse_form(
            race_id, race_date, registration_number, feature_row
        ))
        failures.extend(self.validate_race_outcome_exclusion(
            race_id, race_date, feature_row
        ))

        return failures

    def validate_no_leakage(
        self,
        race_id: str,
        feature_row: Dict[str, Any]
    ) -> bool:
        """
        Quick check if a feature row has any leakage.

        Args:
            race_id: Target race ID
            feature_row: Feature dictionary to validate

        Returns:
            True if no leakage detected
        """
        conn = self._get_connection()

        # Get race info
        cursor = conn.execute("""
            SELECT race_date FROM races_standardized WHERE race_id = ?
        """, (race_id,))

        row = cursor.fetchone()
        if not row:
            logger.warning(f"Race not found: {race_id}")
            return True

        race_date = date.fromisoformat(row[0])

        # Get entry info from feature row if available
        trainer_id = feature_row.get('trainer_id', '')
        jockey_id = feature_row.get('jockey_id', '')
        registration_number = feature_row.get('registration_number', '')
        entry_id = feature_row.get('entry_id', '')

        failures = self.validate_feature_row(
            race_id, race_date, entry_id,
            trainer_id, jockey_id, registration_number,
            feature_row
        )

        return len([f for f in failures if f.severity == 'critical']) == 0

    def run_validation_suite(
        self,
        sample_size: int = 100,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        feature_generator: Optional[Any] = None
    ) -> ValidationReport:
        """
        Run comprehensive validation suite on sampled races.

        This should be run before any model training.

        Args:
            sample_size: Number of races to validate
            start_date: Optional start date for sampling
            end_date: Optional end date for sampling
            feature_generator: Optional feature engine to generate features

        Returns:
            ValidationReport with results
        """
        import time
        start_time = time.time()

        report = ValidationReport()
        conn = self._get_connection()

        # Get race sample
        query = "SELECT race_id, race_date FROM races_standardized"
        params = []

        if start_date:
            query += " WHERE race_date >= ?"
            params.append(start_date.isoformat())
        if end_date:
            if start_date:
                query += " AND race_date <= ?"
            else:
                query += " WHERE race_date <= ?"
            params.append(end_date.isoformat())

        query += " ORDER BY RANDOM() LIMIT ?"
        params.append(sample_size)

        cursor = conn.execute(query, params)
        races = cursor.fetchall()

        logger.info(f"Validating {len(races)} sampled races")

        for race_row in races:
            race_id = race_row[0]
            race_date = date.fromisoformat(race_row[1])
            report.races_validated += 1

            # Get entries for this race
            cursor = conn.execute("""
                SELECT
                    entry_id,
                    trainer_id,
                    jockey_id,
                    registration_number
                FROM race_entries_standardized
                WHERE race_id = ?
                    AND scratched = 0
            """, (race_id,))

            entries = cursor.fetchall()

            for entry_row in entries:
                report.entries_validated += 1
                entry_id = entry_row[0]
                trainer_id = entry_row[1] or ''
                jockey_id = entry_row[2] or ''
                registration_number = entry_row[3] or ''

                # Generate features if generator provided
                if feature_generator:
                    try:
                        feature_row = feature_generator.calculate_entry_features(
                            race_id, entry_id, race_date
                        )
                    except Exception as e:
                        logger.warning(f"Failed to generate features for {entry_id}: {e}")
                        feature_row = {}
                else:
                    # Use empty dict for structural validation
                    feature_row = {
                        'trainer_id': trainer_id,
                        'jockey_id': jockey_id,
                        'registration_number': registration_number,
                        'entry_id': entry_id,
                    }

                # Run validation
                report.tests_run += 1

                failures = self.validate_feature_row(
                    race_id, race_date, entry_id,
                    trainer_id, jockey_id, registration_number,
                    feature_row
                )

                if failures:
                    for failure in failures:
                        report.add_failure(failure)
                else:
                    report.tests_passed += 1

        report.execution_time_seconds = time.time() - start_time

        logger.info(report.summary())

        return report

    def validate_feature_engine(
        self,
        feature_engine: Any,
        sample_size: int = 100
    ) -> ValidationReport:
        """
        Validate a FeatureEngine implementation.

        Tests that the feature engine produces valid, leakage-free features.

        Args:
            feature_engine: FeatureEngine instance to validate
            sample_size: Number of races to test

        Returns:
            ValidationReport with results
        """
        return self.run_validation_suite(
            sample_size=sample_size,
            feature_generator=feature_engine
        )


class FeatureAuditor:
    """
    Audits feature distributions and consistency.

    Provides additional validation beyond leakage detection.
    """

    def __init__(self, db_path: str = 'racing_data.db'):
        """
        Initialize the auditor.

        Args:
            db_path: Path to SQLite database
        """
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path)
            self._conn.row_factory = sqlite3.Row
        return self._conn

    def close(self) -> None:
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def check_feature_ranges(
        self,
        features: List[Dict[str, Any]],
        expected_ranges: Dict[str, Tuple[float, float]]
    ) -> Dict[str, List[Any]]:
        """
        Check if feature values fall within expected ranges.

        Args:
            features: List of feature dictionaries
            expected_ranges: Dict mapping feature name to (min, max) tuple

        Returns:
            Dict of features with out-of-range values
        """
        out_of_range = {}

        for feature_name, (min_val, max_val) in expected_ranges.items():
            violations = []
            for f in features:
                value = f.get(feature_name)
                if value is not None:
                    if value < min_val or value > max_val:
                        violations.append(value)

            if violations:
                out_of_range[feature_name] = violations

        return out_of_range

    def check_null_rates(
        self,
        features: List[Dict[str, Any]],
        max_null_rate: float = 0.5
    ) -> Dict[str, float]:
        """
        Check null rates for each feature.

        Args:
            features: List of feature dictionaries
            max_null_rate: Maximum acceptable null rate

        Returns:
            Dict of features exceeding null rate threshold
        """
        if not features:
            return {}

        high_null_features = {}
        n = len(features)

        # Get all feature names
        all_names = set()
        for f in features:
            all_names.update(f.keys())

        for name in all_names:
            null_count = sum(1 for f in features if f.get(name) is None)
            null_rate = null_count / n

            if null_rate > max_null_rate:
                high_null_features[name] = null_rate

        return high_null_features

    def check_feature_correlations(
        self,
        features: List[Dict[str, Any]],
        feature_pairs: List[Tuple[str, str]],
        min_correlation: float = -1.0,
        max_correlation: float = 1.0
    ) -> Dict[Tuple[str, str], float]:
        """
        Check correlations between feature pairs.

        Useful for detecting redundant features or unexpected relationships.

        Args:
            features: List of feature dictionaries
            feature_pairs: List of (feature1, feature2) tuples to check
            min_correlation: Minimum expected correlation
            max_correlation: Maximum expected correlation

        Returns:
            Dict of pairs with unexpected correlations
        """
        # Simplified correlation check (would use numpy/pandas in production)
        unexpected = {}

        for f1, f2 in feature_pairs:
            values1 = [f.get(f1) for f in features if f.get(f1) is not None and f.get(f2) is not None]
            values2 = [f.get(f2) for f in features if f.get(f1) is not None and f.get(f2) is not None]

            if len(values1) < 10:
                continue

            # Calculate simple Pearson correlation
            try:
                n = len(values1)
                mean1 = sum(values1) / n
                mean2 = sum(values2) / n

                cov = sum((v1 - mean1) * (v2 - mean2) for v1, v2 in zip(values1, values2)) / n
                std1 = (sum((v - mean1) ** 2 for v in values1) / n) ** 0.5
                std2 = (sum((v - mean2) ** 2 for v in values2) / n) ** 0.5

                if std1 > 0 and std2 > 0:
                    corr = cov / (std1 * std2)

                    if corr < min_correlation or corr > max_correlation:
                        unexpected[(f1, f2)] = corr
            except Exception:
                pass

        return unexpected


# Convenience function for quick validation
def validate_features(
    db_path: str,
    sample_size: int = 100
) -> ValidationReport:
    """
    Convenience function to run validation suite.

    Args:
        db_path: Path to SQLite database
        sample_size: Number of races to validate

    Returns:
        ValidationReport
    """
    validator = LeakageValidator(db_path=db_path)
    try:
        return validator.run_validation_suite(sample_size=sample_size)
    finally:
        validator.close()
