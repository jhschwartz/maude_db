#!/usr/bin/env python3
"""
Basic Usage Example for MaudeDatabase

This minimal example shows the core functionality:
- Connecting to the database
- Downloading data
- Running simple queries

Usage:
    python basic_usage.py
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from maude_db import MaudeDatabase


def main():
    # Create or connect to database
    db = MaudeDatabase('maude_example.db', verbose=True)

    # Download data for a single year
    print("\nDownloading 1998 device data...")
    db.add_years(
        years=1998,
        tables=['device'],
        download=True,
        data_dir='./maude_data'
    )

    # Show database info
    print("\nDatabase summary:")
    db.info()

    # Simple query: count all devices
    print("\n" + "="*60)
    result = db.query("SELECT COUNT(*) as total FROM device")
    print(f"Total device records: {result['total'][0]:,}")

    # Query by device name
    print("\n" + "="*60)
    print("Searching for catheter devices...")
    catheters = db.query("""
        SELECT GENERIC_NAME, COUNT(*) as count
        FROM device
        WHERE GENERIC_NAME LIKE '%catheter%'
        GROUP BY GENERIC_NAME
        ORDER BY count DESC
        LIMIT 5
    """)
    print("\nTop 5 catheter types:")
    print(catheters.to_string(index=False))

    # Clean up
    db.close()
    print("\n" + "="*60)
    print("Done!")


if __name__ == '__main__':
    main()