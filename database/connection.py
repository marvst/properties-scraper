"""Database connection management for vou-pra-curitiba SQLite database."""

import os
import sqlite3
from pathlib import Path


def get_database_path() -> str:
    """Get the path to the vou-pra-curitiba SQLite database.

    Reads from VPC_DATABASE_PATH environment variable, or defaults to
    ../vou-pra-curitiba/db/development.sqlite3 relative to procrawl root.
    """
    env_path = os.environ.get("VPC_DATABASE_PATH")
    if env_path:
        return env_path

    # Default: relative path from procrawl directory (Rails 8+ uses storage/)
    procrawl_root = Path(__file__).parent.parent
    default_path = procrawl_root.parent / "app" / "storage" / "development.sqlite3"
    return str(default_path)


def get_connection() -> sqlite3.Connection:
    """Get a SQLite connection to the vou-pra-curitiba database.

    Returns a connection with row_factory set to sqlite3.Row for
    dict-like access to query results.

    Raises:
        FileNotFoundError: If the database file does not exist.
    """
    db_path = get_database_path()

    if not Path(db_path).exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def close_connection(conn: sqlite3.Connection) -> None:
    """Close a database connection."""
    if conn:
        conn.close()
