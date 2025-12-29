#!/usr/bin/env python3
# prepare_zenodo_archive.py - Create complete MAUDE database archive for Zenodo upload
# Copyright (C) 2024 Jacob Schwartz, University of Michigan Medical School
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

"""
Prepare a complete MAUDE database archive for Zenodo upload.

This script creates a comprehensive, reproducible archive of FDA MAUDE data
suitable for long-term archival on Zenodo or other research repositories.

Features:
- Downloads complete MAUDE database (all years, all tables)
- Generates comprehensive metadata and documentation
- Creates schema documentation with data dictionary
- Compresses archive for efficient storage
- Validates data integrity
- Generates DOI-ready README

Usage:
    python prepare_zenodo_archive.py --years 1991-2024 --output maude_full_archive
    python prepare_zenodo_archive.py --years 2020-2024 --tables device master
"""

import argparse
import os
import sys
import json
import gzip
import shutil
import hashlib
from datetime import datetime
from pathlib import Path

# Add parent directory to path to import maude_db
sys.path.insert(0, str(Path(__file__).parent.parent))
from maude_db import MaudeDatabase


def calculate_file_hash(filepath, algorithm='sha256'):
    """Calculate cryptographic hash of a file for integrity verification."""
    hash_func = hashlib.new(algorithm)
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            hash_func.update(chunk)
    return hash_func.hexdigest()


def get_database_stats(db):
    """Collect comprehensive statistics about the database."""
    stats = {
        'creation_timestamp': datetime.now().isoformat(),
        'tables': {}
    }

    # Get table information
    tables_df = db.query("SELECT name FROM sqlite_master WHERE type='table'")
    tables = tables_df['name'].tolist()

    for table in tables:
        table_stats = {
            'row_count': 0,
            'columns': [],
            'sample_data': []
        }

        # Row count
        count_df = db.query(f"SELECT COUNT(*) as count FROM {table}")
        table_stats['row_count'] = int(count_df['count'][0])

        # Column information
        pragma_df = db.query(f"PRAGMA table_info({table})")
        table_stats['columns'] = pragma_df.to_dict('records')

        # Sample data (first 3 rows)
        if table_stats['row_count'] > 0:
            sample_df = db.query(f"SELECT * FROM {table} LIMIT 3")
            table_stats['sample_data'] = sample_df.to_dict('records')

        stats['tables'][table] = table_stats

    # Date range for master table
    if 'master' in tables:
        try:
            date_df = db.query(
                "SELECT MIN(date_received) as first, MAX(date_received) as last FROM master"
            )
            stats['date_range'] = {
                'first_report': date_df['first'][0],
                'last_report': date_df['last'][0]
            }
        except:
            pass

    return stats


def create_readme(output_dir, stats, args):
    """Generate comprehensive README for Zenodo."""
    readme_content = f"""# FDA MAUDE Database Archive

**Created:** {datetime.now().strftime('%Y-%m-%d')}
**Version:** {datetime.now().strftime('%Y.%m.%d')}
**License:** Public Domain (FDA Public Data)

## Description

This archive contains a complete snapshot of the FDA MAUDE (Manufacturer and User
Facility Device Experience) database, downloaded from the official FDA data repository.

The MAUDE database contains adverse event reports for medical devices. This data is
submitted to the FDA by mandatory reporters (manufacturers, importers, and device user
facilities) and voluntary reporters such as healthcare professionals, patients, and consumers.

## Contents

### Database File

- **maude_archive.db** - SQLite database containing all MAUDE data
  - Format: SQLite 3
  - Size: {os.path.getsize(os.path.join(output_dir, 'maude_archive.db')) / (1024**3):.2f} GB
  - Tables: {', '.join(stats['tables'].keys())}

### Documentation

- **README.md** - This file
- **schema.json** - Complete database schema with data dictionary
- **metadata.json** - Archive metadata and statistics
- **checksums.txt** - SHA-256 checksums for integrity verification

## Database Statistics

"""

    # Add table statistics
    for table_name, table_stats in stats['tables'].items():
        readme_content += f"### {table_name.upper()} Table\n\n"
        readme_content += f"- **Records:** {table_stats['row_count']:,}\n"
        readme_content += f"- **Columns:** {len(table_stats['columns'])}\n\n"

    if 'date_range' in stats:
        readme_content += f"\n## Data Coverage\n\n"
        readme_content += f"- **First Report:** {stats['date_range']['first_report']}\n"
        readme_content += f"- **Last Report:** {stats['date_range']['last_report']}\n\n"

    readme_content += f"""
## Usage

### Python with sqlite3

```python
import sqlite3
import pandas as pd

# Connect to database
conn = sqlite3.connect('maude_archive.db')

# Query device events
query = \"\"\"
    SELECT m.*, d.*
    FROM master m
    JOIN device d ON m.mdr_report_key = d.mdr_report_key
    WHERE d.GENERIC_NAME LIKE '%pacemaker%'
\"\"\"
df = pd.read_sql_query(query, conn)
print(f"Found {{len(df)}} pacemaker events")
conn.close()
```

### SQLite Command Line

```bash
# Open database
sqlite3 maude_archive.db

# Run query
SELECT COUNT(*) FROM master;
```

### GUI Tools

Use DB Browser for SQLite (https://sqlitebrowser.org/) or similar tools to
explore the data visually.

## Data Source

**Original Source:** U.S. Food and Drug Administration
**URL:** https://www.accessdata.fda.gov/MAUDE/ftparea
**Download Date:** {stats['creation_timestamp']}

## Schema Information

See `schema.json` for complete database schema including:
- Column names and types
- Sample data for each table
- Data dictionary

## Verification

Verify file integrity using checksums:

```bash
sha256sum -c checksums.txt
```

## Citation

When using this data, please cite both this archive and the original FDA source:

**This Archive:**
```
Schwartz, J. ({datetime.now().year}). FDA MAUDE Database Archive ({datetime.now().strftime('%Y.%m.%d')}) [Data set].
Zenodo. https://doi.org/[DOI-will-be-assigned-by-Zenodo]
```

**Original FDA Source:**
```
U.S. Food and Drug Administration. ({datetime.now().year}). MAUDE - Manufacturer and User
Facility Device Experience Database. Retrieved {datetime.now().strftime('%B %d, %Y')},
from https://www.fda.gov/medical-devices/mandatory-reporting-requirements-manufacturers-importers-and-device-user-facilities/manufacturer-and-user-facility-device-experience-database-maude
```

## Important Notes

### Data Limitations

1. **Reporting Bias:** Not all adverse events are reported to the FDA
2. **Unverified Reports:** Reports are not verified by the FDA
3. **Causality:** Presence in MAUDE does not establish causation
4. **Completeness:** Some fields may be incomplete or missing

### Use Restrictions

This data is in the public domain. However, users should:
- Not use data to make clinical decisions for individual patients
- Understand limitations when drawing conclusions
- Follow ethical guidelines for research use

## Technical Details

- **Database Engine:** SQLite 3
- **Encoding:** UTF-8
- **Original File Format:** Pipe-delimited text files (|)
- **Original File Encoding:** Latin-1 (converted to UTF-8 in database)

## Tools Used

This archive was created using `maude_db` Python library:
https://github.com/jhschwartz/maude_db

## Contact

For questions about this archive, contact:
Jacob Schwartz, University of Michigan Medical School
jaschwa@umich.edu

For questions about the MAUDE database, contact:
FDA CDRH
https://www.fda.gov/medical-devices

## Version History

- **{datetime.now().strftime('%Y.%m.%d')}**: Initial archive creation

## License

This data is in the public domain as U.S. Government work. See FDA website for details.
"""

    with open(os.path.join(output_dir, 'README.md'), 'w') as f:
        f.write(readme_content)


def create_metadata_file(output_dir, stats, args):
    """Create machine-readable metadata file."""
    metadata = {
        'archive_version': datetime.now().strftime('%Y.%m.%d'),
        'creation_date': datetime.now().isoformat(),
        'creator': 'Jacob Schwartz, University of Michigan Medical School',
        'source': {
            'name': 'FDA MAUDE Database',
            'url': 'https://www.accessdata.fda.gov/MAUDE/ftparea',
            'download_date': datetime.now().isoformat()
        },
        'database': {
            'format': 'SQLite 3',
            'filename': 'maude_archive.db',
            'size_bytes': os.path.getsize(os.path.join(output_dir, 'maude_archive.db')),
            'tables': list(stats['tables'].keys()),
            'total_records': sum(t['row_count'] for t in stats['tables'].values())
        },
        'parameters': {
            'years': args.years,
            'tables': args.tables
        },
        'statistics': stats
    }

    with open(os.path.join(output_dir, 'metadata.json'), 'w') as f:
        json.dump(metadata, f, indent=2, default=str)


def create_checksums(output_dir):
    """Generate SHA-256 checksums for all files."""
    checksums = []

    for filename in ['maude_archive.db', 'README.md', 'schema.json', 'metadata.json']:
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath):
            file_hash = calculate_file_hash(filepath)
            checksums.append(f"{file_hash}  {filename}")

    with open(os.path.join(output_dir, 'checksums.txt'), 'w') as f:
        f.write('\n'.join(checksums))


def main():
    parser = argparse.ArgumentParser(
        description='Prepare FDA MAUDE database archive for Zenodo upload',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full archive (all years, all tables)
  python prepare_zenodo_archive.py --years all --output maude_full_archive

  # Recent years only
  python prepare_zenodo_archive.py --years 2020-2024 --output maude_recent

  # Specific tables only
  python prepare_zenodo_archive.py --years 2015-2024 --tables device master --output maude_device_only
        """
    )

    parser.add_argument('--years', default='all',
                        help='Years to include (e.g., "all", "2020-2024", "2020,2021,2022")')
    parser.add_argument('--tables', nargs='+',
                        default=['master', 'device', 'text', 'patient'],
                        help='Tables to include (default: all main tables)')
    parser.add_argument('--output', default='maude_archive',
                        help='Output directory name (default: maude_archive)')
    parser.add_argument('--compress', action='store_true',
                        help='Compress database with gzip (for very large archives)')
    parser.add_argument('--no-download', action='store_true',
                        help='Use existing maude_data directory instead of downloading')

    args = parser.parse_args()

    # Create output directory
    output_dir = args.output
    os.makedirs(output_dir, exist_ok=True)

    print("="*70)
    print("FDA MAUDE DATABASE ARCHIVE PREPARATION FOR ZENODO")
    print("="*70)
    print(f"\nConfiguration:")
    print(f"  Years: {args.years}")
    print(f"  Tables: {', '.join(args.tables)}")
    print(f"  Output: {output_dir}/")
    print(f"  Download: {not args.no_download}")
    print(f"  Compress: {args.compress}")
    print()

    # Create database
    db_path = os.path.join(output_dir, 'maude_archive.db')

    print("Step 1: Creating database and downloading data...")
    print("-" * 70)

    with MaudeDatabase(db_path, verbose=True) as db:
        db.add_years(
            args.years,
            tables=args.tables,
            download=not args.no_download,
            interactive=True
        )

        print("\nStep 2: Collecting database statistics...")
        print("-" * 70)
        stats = get_database_stats(db)

        print(f"\nDatabase contains {sum(t['row_count'] for t in stats['tables'].values()):,} total records")
        for table_name, table_stats in stats['tables'].items():
            print(f"  {table_name}: {table_stats['row_count']:,} records")

    print("\nStep 3: Generating documentation...")
    print("-" * 70)

    # Create schema documentation
    with open(os.path.join(output_dir, 'schema.json'), 'w') as f:
        json.dump(stats['tables'], f, indent=2, default=str)
    print("  ✓ schema.json")

    # Create README
    create_readme(output_dir, stats, args)
    print("  ✓ README.md")

    # Create metadata
    create_metadata_file(output_dir, stats, args)
    print("  ✓ metadata.json")

    # Create checksums
    create_checksums(output_dir)
    print("  ✓ checksums.txt")

    # Compress if requested
    if args.compress:
        print("\nStep 4: Compressing database...")
        print("-" * 70)
        with open(db_path, 'rb') as f_in:
            with gzip.open(f"{db_path}.gz", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        compressed_size = os.path.getsize(f"{db_path}.gz")
        original_size = os.path.getsize(db_path)
        ratio = (1 - compressed_size / original_size) * 100

        print(f"  Original: {original_size / (1024**3):.2f} GB")
        print(f"  Compressed: {compressed_size / (1024**3):.2f} GB")
        print(f"  Savings: {ratio:.1f}%")

        # Update checksums to include compressed file
        file_hash = calculate_file_hash(f"{db_path}.gz")
        with open(os.path.join(output_dir, 'checksums.txt'), 'a') as f:
            f.write(f"\n{file_hash}  maude_archive.db.gz")

    print("\n" + "="*70)
    print("ARCHIVE PREPARATION COMPLETE")
    print("="*70)
    print(f"\nArchive location: {os.path.abspath(output_dir)}/")
    print(f"\nContents:")
    for filename in os.listdir(output_dir):
        filepath = os.path.join(output_dir, filename)
        size = os.path.getsize(filepath)
        if size > 1024**3:
            size_str = f"{size / (1024**3):.2f} GB"
        elif size > 1024**2:
            size_str = f"{size / (1024**2):.2f} MB"
        else:
            size_str = f"{size / 1024:.2f} KB"
        print(f"  {filename:30} {size_str:>12}")

    print(f"\nTotal archive size: {sum(os.path.getsize(os.path.join(output_dir, f)) for f in os.listdir(output_dir)) / (1024**3):.2f} GB")

    print("\n" + "="*70)
    print("NEXT STEPS FOR ZENODO UPLOAD")
    print("="*70)
    print("""
1. Review the generated README.md and verify all information is correct

2. Create a Zenodo account (if you don't have one):
   https://zenodo.org/signup/

3. Create a new upload:
   - Go to https://zenodo.org/deposit/new
   - Upload all files from the archive directory
   - Fill in metadata (title, creators, description, keywords)
   - Select license: "Creative Commons Zero v1.0 Universal" or "Public Domain"
   - Add related identifiers (original FDA source)

4. Recommended metadata for Zenodo:

   Title: FDA MAUDE Database Archive (YYYY.MM.DD)

   Description: Complete snapshot of the FDA MAUDE (Manufacturer and User
   Facility Device Experience) database containing adverse event reports for
   medical devices. Downloaded from official FDA data repository.

   Keywords: FDA, MAUDE, medical devices, adverse events, safety surveillance

   Related identifiers:
   - https://www.accessdata.fda.gov/MAUDE/ftparea (isSourceOf)

5. After publishing, add the DOI to your citations and documentation

6. Consider creating a GitHub release linking to the Zenodo archive
""")


if __name__ == '__main__':
    main()
