"""
Database Configuration Module
Version: 1.0

Provides database connection management for both PostgreSQL and SQLite.
Supports connection pooling, environment-based configuration, and migration running.

Usage:
    from config.database import get_database_engine, get_session

    # Get SQLAlchemy engine
    engine = get_database_engine()

    # Get session for database operations
    with get_session() as session:
        results = session.execute("SELECT * FROM racing.races LIMIT 10")

Environment Variables:
    DB_TYPE: 'postgresql' or 'sqlite' (default: from config.yaml)
    DB_HOST: PostgreSQL host (default: localhost)
    DB_PORT: PostgreSQL port (default: 5432)
    DB_NAME: Database name (default: racing_db)
    DB_USER: Database user (default: racing_app)
    DB_PASSWORD: Database password (required for production)
    DB_SCHEMA: PostgreSQL schema (default: racing)
    SQLITE_PATH: Path to SQLite database (default: racing_data.db)
"""

import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import yaml

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import Engine
    from sqlalchemy.orm import Session, sessionmaker
    from sqlalchemy.pool import QueuePool
except ImportError:
    raise ImportError("SQLAlchemy not installed. Run: pip install sqlalchemy psycopg2-binary")

# Configure logging
logger = logging.getLogger(__name__)

# Module-level engine cache
_engine: Optional[Engine] = None
_session_factory: Optional[sessionmaker] = None


def load_config() -> Dict[str, Any]:
    """
    Load database configuration from config.yaml.

    Returns:
        Dict containing database configuration
    """
    config_path = Path(__file__).parent / 'config.yaml'

    if not config_path.exists():
        # Try parent directory
        config_path = Path(__file__).parent.parent / 'config' / 'config.yaml'

    if not config_path.exists():
        logger.warning("config.yaml not found, using defaults")
        return {}

    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    return config.get('database', {})


def get_env_var(name: str, default: Optional[str] = None) -> Optional[str]:
    """
    Get environment variable with config.yaml style variable substitution.

    Supports ${VAR_NAME:default} syntax in config values.

    Args:
        name: Environment variable name
        default: Default value if not set

    Returns:
        Environment variable value or default
    """
    value = os.environ.get(name)
    if value is not None:
        return value
    return default


def build_postgresql_url(config: Dict[str, Any]) -> str:
    """
    Build PostgreSQL connection URL from configuration.

    Args:
        config: Database configuration dictionary

    Returns:
        PostgreSQL connection URL string
    """
    pg_config = config.get('postgresql', {})

    host = get_env_var('DB_HOST', pg_config.get('host', 'localhost'))
    port = get_env_var('DB_PORT', str(pg_config.get('port', 5432)))
    database = get_env_var('DB_NAME', pg_config.get('database', 'racing_db'))
    user = get_env_var('DB_USER', 'racing_app')
    password = get_env_var('DB_PASSWORD', '')

    if not password:
        logger.warning("DB_PASSWORD not set, connection may fail")

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def build_sqlite_url(config: Dict[str, Any]) -> str:
    """
    Build SQLite connection URL from configuration.

    Args:
        config: Database configuration dictionary

    Returns:
        SQLite connection URL string
    """
    sqlite_config = config.get('sqlite', {})
    db_path = get_env_var('SQLITE_PATH', sqlite_config.get('path', 'racing_data.db'))

    # Make path absolute if relative
    if not Path(db_path).is_absolute():
        db_path = str(Path(__file__).parent.parent / db_path)

    return f"sqlite:///{db_path}"


def get_database_url(config: Optional[Dict[str, Any]] = None) -> str:
    """
    Get database connection URL based on configuration.

    Args:
        config: Optional database configuration dictionary

    Returns:
        Database connection URL string
    """
    if config is None:
        config = load_config()

    db_type = get_env_var('DB_TYPE', config.get('type', 'sqlite'))

    if db_type == 'postgresql':
        return build_postgresql_url(config)
    elif db_type == 'sqlite':
        return build_sqlite_url(config)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def create_database_engine(
    url: Optional[str] = None,
    pool_size: int = 10,
    max_overflow: int = 20,
    echo: bool = False
) -> Engine:
    """
    Create SQLAlchemy database engine with connection pooling.

    Args:
        url: Database connection URL (uses config if not provided)
        pool_size: Connection pool size (PostgreSQL only)
        max_overflow: Max overflow connections (PostgreSQL only)
        echo: Enable SQL logging

    Returns:
        SQLAlchemy Engine instance
    """
    if url is None:
        url = get_database_url()

    engine_kwargs: Dict[str, Any] = {
        'echo': echo,
    }

    # Configure connection pooling for PostgreSQL
    if url.startswith('postgresql'):
        engine_kwargs.update({
            'poolclass': QueuePool,
            'pool_size': pool_size,
            'max_overflow': max_overflow,
            'pool_pre_ping': True,  # Test connections before use
            'pool_recycle': 3600,   # Recycle connections after 1 hour
        })

        # Set default schema
        schema = get_env_var('DB_SCHEMA', 'racing')
        engine_kwargs['connect_args'] = {
            'options': f'-c search_path={schema},features,models,betting,monitoring,public'
        }

    logger.info(f"Creating database engine: {url.split('@')[-1] if '@' in url else url}")
    return create_engine(url, **engine_kwargs)


def get_database_engine() -> Engine:
    """
    Get or create the global database engine.

    Returns:
        SQLAlchemy Engine instance
    """
    global _engine

    if _engine is None:
        _engine = create_database_engine()

    return _engine


def get_session_factory() -> sessionmaker:
    """
    Get or create the session factory.

    Returns:
        SQLAlchemy sessionmaker instance
    """
    global _session_factory

    if _session_factory is None:
        engine = get_database_engine()
        _session_factory = sessionmaker(bind=engine)

    return _session_factory


@contextmanager
def get_session() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.

    Yields:
        SQLAlchemy Session instance

    Example:
        with get_session() as session:
            results = session.execute(text("SELECT * FROM racing.races"))
    """
    factory = get_session_factory()
    session = factory()

    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def test_connection() -> bool:
    """
    Test database connection.

    Returns:
        True if connection successful, False otherwise
    """
    try:
        engine = get_database_engine()
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
            logger.info("Database connection test successful")
            return True
    except Exception as e:
        logger.error(f"Database connection test failed: {e}")
        return False


def get_table_counts() -> Dict[str, int]:
    """
    Get row counts for all racing tables.

    Returns:
        Dict mapping table names to row counts
    """
    tables = [
        'racing.horses_master',
        'racing.trainers',
        'racing.jockeys',
        'racing.owners',
        'racing.races',
        'racing.race_entries',
    ]

    counts = {}

    with get_session() as session:
        for table in tables:
            try:
                result = session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                counts[table] = result.scalar()
            except Exception as e:
                logger.warning(f"Could not count {table}: {e}")
                counts[table] = -1

    return counts


def run_migrations(migrations_dir: Optional[str] = None) -> bool:
    """
    Run pending database migrations.

    Args:
        migrations_dir: Path to migrations directory

    Returns:
        True if all migrations successful
    """
    if migrations_dir is None:
        migrations_dir = str(Path(__file__).parent.parent / 'database' / 'migrations')

    migrations_path = Path(migrations_dir)

    if not migrations_path.exists():
        logger.error(f"Migrations directory not found: {migrations_path}")
        return False

    # Get list of SQL migration files
    migration_files = sorted(migrations_path.glob('*.sql'))

    if not migration_files:
        logger.info("No SQL migration files found")
        return True

    engine = get_database_engine()

    # Get already applied migrations
    applied = set()
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version FROM public.schema_migrations"))
            applied = {row[0] for row in result}
    except Exception:
        logger.info("schema_migrations table does not exist, will be created")

    # Run pending migrations
    for migration_file in migration_files:
        version = migration_file.stem.split('_')[0]

        if version in applied:
            logger.info(f"Migration {version} already applied, skipping")
            continue

        logger.info(f"Running migration: {migration_file.name}")

        try:
            with open(migration_file, 'r') as f:
                sql = f.read()

            with engine.connect() as conn:
                # Execute migration in a transaction
                trans = conn.begin()
                try:
                    # Split by semicolons and execute each statement
                    for statement in sql.split(';'):
                        statement = statement.strip()
                        if statement and not statement.startswith('--'):
                            conn.execute(text(statement))
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    raise e

            logger.info(f"Migration {version} completed successfully")

        except Exception as e:
            logger.error(f"Migration {version} failed: {e}")
            return False

    logger.info("All migrations completed successfully")
    return True


class DatabaseManager:
    """
    High-level database management class.

    Provides methods for common database operations.
    """

    def __init__(self, url: Optional[str] = None):
        """
        Initialize database manager.

        Args:
            url: Database connection URL (uses config if not provided)
        """
        self.url = url or get_database_url()
        self.engine = create_database_engine(self.url)
        self.session_factory = sessionmaker(bind=self.engine)

    @contextmanager
    def session(self) -> Generator[Session, None, None]:
        """Get a session context manager."""
        session = self.session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def execute_query(self, query: str, params: Optional[Dict] = None) -> list:
        """
        Execute a query and return results.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            List of result rows
        """
        with self.session() as session:
            result = session.execute(text(query), params or {})
            return result.fetchall()

    def execute_update(self, query: str, params: Optional[Dict] = None) -> int:
        """
        Execute an update query.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Number of affected rows
        """
        with self.session() as session:
            result = session.execute(text(query), params or {})
            return result.rowcount

    def get_race(self, race_id: str) -> Optional[Dict]:
        """
        Get race by ID.

        Args:
            race_id: Race identifier

        Returns:
            Race data dictionary or None
        """
        query = """
            SELECT * FROM racing.races
            WHERE race_id = :race_id
        """
        results = self.execute_query(query, {'race_id': race_id})
        return dict(results[0]) if results else None

    def get_race_entries(self, race_id: str) -> list:
        """
        Get all entries for a race.

        Args:
            race_id: Race identifier

        Returns:
            List of entry dictionaries
        """
        query = """
            SELECT * FROM racing.vw_race_entries_complete
            WHERE race_id = :race_id
            ORDER BY post_position
        """
        results = self.execute_query(query, {'race_id': race_id})
        return [dict(row) for row in results]

    def get_horse_history(self, registration_number: str, limit: int = 10) -> list:
        """
        Get recent race history for a horse.

        Args:
            registration_number: Horse registration number
            limit: Maximum number of races to return

        Returns:
            List of race entry dictionaries
        """
        query = """
            SELECT * FROM racing.vw_race_entries_complete
            WHERE registration_number = :reg_num
            ORDER BY race_date DESC
            LIMIT :limit
        """
        results = self.execute_query(query, {
            'reg_num': registration_number,
            'limit': limit
        })
        return [dict(row) for row in results]


# Entry point for command-line testing
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Database configuration utility')
    parser.add_argument('--test', action='store_true', help='Test database connection')
    parser.add_argument('--counts', action='store_true', help='Show table row counts')
    parser.add_argument('--migrate', action='store_true', help='Run pending migrations')
    parser.add_argument('--url', type=str, help='Override database URL')

    args = parser.parse_args()

    # Set log level
    logging.basicConfig(level=logging.INFO)

    if args.url:
        os.environ['DB_URL'] = args.url

    if args.test:
        success = test_connection()
        exit(0 if success else 1)

    if args.counts:
        counts = get_table_counts()
        print("\nTable Row Counts:")
        print("-" * 40)
        for table, count in counts.items():
            print(f"  {table}: {count:,}")

    if args.migrate:
        success = run_migrations()
        exit(0 if success else 1)

    if not any([args.test, args.counts, args.migrate]):
        print("Database URL:", get_database_url())
        print("\nUse --test, --counts, or --migrate for actions")
