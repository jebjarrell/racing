#!/usr/bin/env python3
"""
Database creation script for Racing Pipeline
Creates SQLite database with tables for horse, trainer, and owner data
"""

import sqlite3
import os
from datetime import datetime

def create_database():
    """Create the racing database and tables"""
    
    # Create database connection
    db_path = 'racing_data.db'
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print(f"Creating database: {db_path}")
    
    # Read and execute SQL schema
    with open('create_tables.sql', 'r') as sql_file:
        sql_commands = sql_file.read()
    
    # Execute all SQL commands
    cursor.executescript(sql_commands)
    
    # Commit changes
    conn.commit()
    
    # Verify tables were created
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print("\nCreated tables:")
    for table in tables:
        print(f"  - {table[0]}")
    
    # Show table schema for verification
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        print(f"\n{table_name} columns:")
        for col in columns:
            print(f"  {col[1]} {col[2]} {'NOT NULL' if col[3] else ''} {'PK' if col[5] else ''}")
    
    conn.close()
    print(f"\nDatabase created successfully: {os.path.abspath(db_path)}")

if __name__ == "__main__":
    create_database()