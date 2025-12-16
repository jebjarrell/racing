"""
Migration 005: Migrate SQLite Data to PostgreSQL
Version: 1.0
Description: Migrates existing racing data from SQLite to PostgreSQL

This script handles the data migration from the SQLite database (racing_data.db)
to the new PostgreSQL database with proper data type conversions and validation.

Usage:
    python 005_migrate_sqlite_data.py --sqlite-path ../racing_data.db --pg-connection postgresql://...
"""

import argparse
import logging
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

try:
    import psycopg2
    from psycopg2.extras import execute_batch
except ImportError:
    print("Error: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SQLiteToPostgresMigrator:
    """Handles migration of data from SQLite to PostgreSQL."""

    # Table mapping: SQLite table -> PostgreSQL schema.table
    TABLE_MAPPING = {
        'horses_master': 'racing.horses_master',
        'trainers': 'racing.trainers',
        'owners': 'racing.owners',
        'races_standardized': 'racing.races',
        'race_entries_standardized': 'racing.race_entries',
        'horse_race_equipment': 'racing.horse_race_equipment',
        'horse_race_medication': 'racing.horse_race_medication',
        'race_fractions': 'racing.race_fractions',
        'horse_position_calls': 'racing.horse_position_calls',
        'race_wagering': 'racing.race_wagering',
    }

    # Migration order (respects foreign key dependencies)
    MIGRATION_ORDER = [
        'horses_master',
        'trainers',
        'owners',
        'races_standardized',
        'race_entries_standardized',
        'horse_race_equipment',
        'race_fractions',
        'horse_position_calls',
        'race_wagering',
    ]

    def __init__(self, sqlite_path: str, pg_connection: str):
        """
        Initialize the migrator.

        Args:
            sqlite_path: Path to SQLite database file
            pg_connection: PostgreSQL connection string
        """
        self.sqlite_path = Path(sqlite_path)
        self.pg_connection = pg_connection
        self.sqlite_conn: Optional[sqlite3.Connection] = None
        self.pg_conn: Optional[Any] = None
        self.stats: Dict[str, Dict[str, int]] = {}

    def connect(self) -> None:
        """Establish connections to both databases."""
        logger.info(f"Connecting to SQLite: {self.sqlite_path}")
        if not self.sqlite_path.exists():
            raise FileNotFoundError(f"SQLite database not found: {self.sqlite_path}")

        self.sqlite_conn = sqlite3.connect(str(self.sqlite_path))
        self.sqlite_conn.row_factory = sqlite3.Row

        logger.info("Connecting to PostgreSQL...")
        self.pg_conn = psycopg2.connect(self.pg_connection)
        self.pg_conn.autocommit = False

        logger.info("Connections established successfully")

    def disconnect(self) -> None:
        """Close database connections."""
        if self.sqlite_conn:
            self.sqlite_conn.close()
        if self.pg_conn:
            self.pg_conn.close()
        logger.info("Connections closed")

    def get_sqlite_table_count(self, table: str) -> int:
        """Get row count from SQLite table."""
        cursor = self.sqlite_conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        return cursor.fetchone()[0]

    def get_sqlite_columns(self, table: str) -> List[str]:
        """Get column names from SQLite table."""
        cursor = self.sqlite_conn.cursor()
        cursor.execute(f"PRAGMA table_info({table})")
        return [row[1] for row in cursor.fetchall()]

    def get_pg_columns(self, schema: str, table: str) -> List[str]:
        """Get column names from PostgreSQL table."""
        cursor = self.pg_conn.cursor()
        cursor.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema, table))
        return [row[0] for row in cursor.fetchall()]

    def map_columns(self, sqlite_table: str, pg_table: str) -> Tuple[List[str], List[str]]:
        """
        Map SQLite columns to PostgreSQL columns.
        Returns tuple of (common_columns, sqlite_only_columns)
        """
        schema, table = pg_table.split('.')
        sqlite_cols = set(self.get_sqlite_columns(sqlite_table))
        pg_cols = set(self.get_pg_columns(schema, table))

        common = sorted(sqlite_cols.intersection(pg_cols))
        sqlite_only = sorted(sqlite_cols - pg_cols)

        return common, sqlite_only

    def transform_row(self, row: sqlite3.Row, columns: List[str]) -> tuple:
        """Transform a SQLite row for PostgreSQL insertion."""
        values = []
        for col in columns:
            value = row[col]

            # Handle None values
            if value is None:
                values.append(None)
                continue

            # Handle boolean conversion
            if isinstance(value, int) and col.startswith(('has_', 'is_', 'fillies_', 'colts_',
                                                          'mares_', 'geldings_', 'scratched',
                                                          'lasix_first', 'blinkers_')):
                values.append(bool(value))
            else:
                values.append(value)

        return tuple(values)

    def migrate_table(self, sqlite_table: str, batch_size: int = 1000) -> Dict[str, int]:
        """
        Migrate a single table from SQLite to PostgreSQL.

        Args:
            sqlite_table: Name of the SQLite table
            batch_size: Number of rows to insert per batch

        Returns:
            Dict with migration statistics
        """
        pg_table = self.TABLE_MAPPING.get(sqlite_table)
        if not pg_table:
            logger.warning(f"No PostgreSQL mapping for table: {sqlite_table}")
            return {'skipped': 1}

        logger.info(f"Migrating {sqlite_table} -> {pg_table}")

        # Get column mapping
        common_cols, sqlite_only = self.map_columns(sqlite_table, pg_table)

        if sqlite_only:
            logger.warning(f"  Columns in SQLite but not PostgreSQL: {sqlite_only}")

        if not common_cols:
            logger.error(f"  No common columns found for {sqlite_table}")
            return {'error': 1}

        # Count source rows
        source_count = self.get_sqlite_table_count(sqlite_table)
        logger.info(f"  Source rows: {source_count}")

        if source_count == 0:
            return {'source': 0, 'inserted': 0}

        # Prepare insert statement
        col_list = ', '.join(common_cols)
        placeholders = ', '.join(['%s'] * len(common_cols))
        insert_sql = f"""
            INSERT INTO {pg_table} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
        """

        # Fetch and insert in batches
        sqlite_cursor = self.sqlite_conn.cursor()
        pg_cursor = self.pg_conn.cursor()

        sqlite_cursor.execute(f"SELECT * FROM {sqlite_table}")

        inserted = 0
        batch = []

        for row in sqlite_cursor:
            transformed = self.transform_row(row, common_cols)
            batch.append(transformed)

            if len(batch) >= batch_size:
                execute_batch(pg_cursor, insert_sql, batch)
                inserted += len(batch)
                batch = []

                if inserted % 10000 == 0:
                    logger.info(f"  Processed {inserted}/{source_count} rows...")

        # Insert remaining rows
        if batch:
            execute_batch(pg_cursor, insert_sql, batch)
            inserted += len(batch)

        self.pg_conn.commit()

        logger.info(f"  Inserted {inserted} rows")

        return {
            'source': source_count,
            'inserted': inserted,
            'skipped_columns': len(sqlite_only)
        }

    def create_jockeys_from_trainers(self) -> None:
        """
        Create jockey records from trainer references.

        In the original schema, jockeys were stored in the trainers table.
        This creates separate entries in the jockeys table.
        """
        logger.info("Creating jockey records...")

        pg_cursor = self.pg_conn.cursor()

        # Get unique jockey_ids from race_entries
        pg_cursor.execute("""
            INSERT INTO racing.jockeys (external_party_id, first_name, last_name)
            SELECT DISTINCT
                re.jockey_id,
                COALESCE(t.first_name, 'Unknown'),
                COALESCE(t.last_name, 'Jockey')
            FROM racing.race_entries re
            LEFT JOIN racing.trainers t ON re.jockey_id = t.external_party_id
            WHERE re.jockey_id IS NOT NULL
            ON CONFLICT (external_party_id) DO NOTHING
        """)

        jockey_count = pg_cursor.rowcount
        self.pg_conn.commit()

        logger.info(f"  Created {jockey_count} jockey records")

    def update_field_sizes(self) -> None:
        """Calculate and update field_size for all races."""
        logger.info("Updating field sizes...")

        pg_cursor = self.pg_conn.cursor()

        pg_cursor.execute("""
            UPDATE racing.races r
            SET field_size = (
                SELECT COUNT(*)
                FROM racing.race_entries re
                WHERE re.race_id = r.race_id
                AND re.scratched = FALSE
            )
        """)

        updated = pg_cursor.rowcount
        self.pg_conn.commit()

        logger.info(f"  Updated field_size for {updated} races")

    def verify_migration(self) -> bool:
        """Verify data integrity after migration."""
        logger.info("Verifying migration...")

        errors = []
        pg_cursor = self.pg_conn.cursor()

        # Check row counts
        for sqlite_table, pg_table in self.TABLE_MAPPING.items():
            if sqlite_table not in self.MIGRATION_ORDER:
                continue

            sqlite_count = self.get_sqlite_table_count(sqlite_table)

            pg_cursor.execute(f"SELECT COUNT(*) FROM {pg_table}")
            pg_count = pg_cursor.fetchone()[0]

            if pg_count < sqlite_count * 0.95:  # Allow 5% loss due to FK violations
                errors.append(f"{sqlite_table}: {sqlite_count} -> {pg_count} (potential data loss)")
            else:
                logger.info(f"  {pg_table}: {pg_count} rows (source: {sqlite_count})")

        # Check referential integrity
        pg_cursor.execute("""
            SELECT COUNT(*) FROM racing.race_entries re
            LEFT JOIN racing.races r ON re.race_id = r.race_id
            WHERE r.race_id IS NULL
        """)
        orphan_entries = pg_cursor.fetchone()[0]
        if orphan_entries > 0:
            errors.append(f"Found {orphan_entries} orphan race entries")

        if errors:
            logger.error("Migration verification failed:")
            for error in errors:
                logger.error(f"  - {error}")
            return False

        logger.info("Migration verification passed")
        return True

    def run_migration(self) -> bool:
        """Execute the full migration."""
        start_time = datetime.now()
        logger.info(f"Starting migration at {start_time}")

        try:
            self.connect()

            # Migrate tables in order
            for table in self.MIGRATION_ORDER:
                self.stats[table] = self.migrate_table(table)

            # Post-migration tasks
            self.create_jockeys_from_trainers()
            self.update_field_sizes()

            # Verify migration
            success = self.verify_migration()

            # Record migration
            if success:
                pg_cursor = self.pg_conn.cursor()
                pg_cursor.execute("""
                    INSERT INTO public.schema_migrations (version, description)
                    VALUES ('005', 'Migrate SQLite data')
                    ON CONFLICT (version) DO NOTHING
                """)
                self.pg_conn.commit()

            end_time = datetime.now()
            duration = end_time - start_time
            logger.info(f"Migration completed in {duration}")

            # Print summary
            logger.info("\nMigration Summary:")
            logger.info("-" * 50)
            for table, stats in self.stats.items():
                logger.info(f"  {table}: {stats}")

            return success

        except Exception as e:
            logger.error(f"Migration failed: {e}")
            if self.pg_conn:
                self.pg_conn.rollback()
            raise

        finally:
            self.disconnect()


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Migrate SQLite data to PostgreSQL')

    parser.add_argument(
        '--sqlite-path',
        type=str,
        default='../racing_data.db',
        help='Path to SQLite database file'
    )

    parser.add_argument(
        '--pg-connection',
        type=str,
        required=True,
        help='PostgreSQL connection string (e.g., postgresql://user:pass@host:5432/racing_db)'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for inserts (default: 1000)'
    )

    parser.add_argument(
        '--verify-only',
        action='store_true',
        help='Only verify existing migration, do not migrate'
    )

    args = parser.parse_args()

    migrator = SQLiteToPostgresMigrator(args.sqlite_path, args.pg_connection)

    if args.verify_only:
        migrator.connect()
        success = migrator.verify_migration()
        migrator.disconnect()
    else:
        success = migrator.run_migration()

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
