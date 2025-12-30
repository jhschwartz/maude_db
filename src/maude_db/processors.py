# processors.py - MAUDE Database Data Processors
# Copyright (C) 2026 Jacob Schwartz <jaschwa@umich.edu>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Data processing utilities for MAUDE database files.

Handles reading, parsing, and loading MAUDE data files into SQLite database.
"""

import pandas as pd


def process_file(filepath, table_name, conn, chunk_size, verbose=False):
    """
    Read MAUDE text file and insert into SQLite database.

    Args:
        filepath: Path to pipe-delimited text file
        table_name: Name of table to insert into
        conn: SQLite database connection
        chunk_size: Number of rows to process at once
        verbose: Whether to print progress messages
    """
    total_rows = 0

    for i, chunk in enumerate(pd.read_csv(
        filepath,
        sep='|',
        encoding='latin1',
        on_bad_lines='skip',
        chunksize=chunk_size,
        low_memory=False
    )):
        chunk.to_sql(table_name, conn, if_exists='append', index=False)
        total_rows += len(chunk)

        if verbose and i % 10 == 0 and i > 0:
            print(f'    Processed {total_rows:,} rows...')

    if verbose:
        print(f'    Total: {total_rows:,} rows')


def process_cumulative_file(filepath, table_name, year, metadata, conn, chunk_size, verbose=False):
    """
    Read cumulative MAUDE file and insert only specified year into database.

    For tables like master and patient that are distributed as cumulative files,
    this filters to only include records from the specified year.

    Args:
        filepath: Path to pipe-delimited text file
        table_name: Name of table to insert into
        year: Year to filter for
        metadata: Table metadata dict containing date_column info
        conn: SQLite database connection
        chunk_size: Number of rows to process at once
        verbose: Whether to print progress messages
    """
    total_rows = 0
    filtered_rows = 0

    # Fallback to regular processing if no date column defined
    if 'date_column' not in metadata:
        return process_file(filepath, table_name, conn, chunk_size, verbose)

    date_col = metadata['date_column']

    if verbose:
        print(f'    Processing cumulative file, filtering for year {year}...')

    for i, chunk in enumerate(pd.read_csv(
        filepath,
        sep='|',
        encoding='latin1',
        on_bad_lines='skip',
        chunksize=chunk_size,
        low_memory=False
    )):
        total_rows += len(chunk)

        # Filter to specified year
        if date_col in chunk.columns:
            # Extract year from date column
            chunk['_year'] = pd.to_datetime(chunk[date_col], errors='coerce').dt.year
            chunk_filtered = chunk[chunk['_year'] == year]
            chunk_filtered = chunk_filtered.drop(columns=['_year'])
        else:
            if verbose and i == 0:
                print(f'    Warning: Date column {date_col} not found, loading all data')
            chunk_filtered = chunk

        if len(chunk_filtered) > 0:
            chunk_filtered.to_sql(table_name, conn, if_exists='append', index=False)
            filtered_rows += len(chunk_filtered)

        if verbose and i % 10 == 0 and i > 0:
            print(f'    Scanned {total_rows:,} rows, kept {filtered_rows:,}...')

    if verbose:
        print(f'    Total: Scanned {total_rows:,} rows, loaded {filtered_rows:,} rows for year {year}')


def process_cumulative_file_batch(filepath, table_name, years_list, metadata, conn, chunk_size, verbose=False):
    """
    Process cumulative file once for multiple years (batch optimization).

    This is a performance optimization that reads a cumulative file once and filters
    for multiple years simultaneously, instead of reading the same file multiple times.

    Args:
        filepath: Path to pipe-delimited text file
        table_name: Name of table to insert into
        years_list: List of years to extract (e.g., [1996, 1997, ..., 2024])
        metadata: Table metadata dict containing date_column info
        conn: SQLite database connection
        chunk_size: Number of rows to process at once
        verbose: Whether to print progress messages
    """
    if not years_list:
        return

    total_rows = 0
    year_counts = {year: 0 for year in years_list}
    years_set = set(years_list)

    # Fallback to regular processing if no date column defined
    if 'date_column' not in metadata:
        return process_file(filepath, table_name, conn, chunk_size, verbose)

    date_col = metadata['date_column']

    if verbose:
        year_range = f"{min(years_list)}-{max(years_list)}" if len(years_list) > 1 else str(years_list[0])
        print(f'    Processing cumulative file for years {year_range} (batch mode)...')

    for i, chunk in enumerate(pd.read_csv(
        filepath,
        sep='|',
        encoding='latin1',
        on_bad_lines='skip',
        chunksize=chunk_size,
        low_memory=False
    )):
        total_rows += len(chunk)

        # Filter to ANY requested year
        if date_col in chunk.columns:
            # Extract year from date column
            chunk['_year'] = pd.to_datetime(chunk[date_col], errors='coerce').dt.year

            # Filter for any year in the requested set
            chunk_filtered = chunk[chunk['_year'].isin(years_set)]

            # Track per-year counts
            for year in chunk_filtered['_year'].unique():
                if year in year_counts:
                    year_counts[year] += sum(chunk_filtered['_year'] == year)

            chunk_filtered = chunk_filtered.drop(columns=['_year'])
        else:
            if verbose and i == 0:
                print(f'    Warning: Date column {date_col} not found, loading all data')
            chunk_filtered = chunk

        if len(chunk_filtered) > 0:
            chunk_filtered.to_sql(table_name, conn, if_exists='append', index=False)

        if verbose and i % 10 == 0 and i > 0:
            total_kept = sum(year_counts.values())
            print(f'    Scanned {total_rows:,} rows, kept {total_kept:,}...')

    if verbose:
        total_kept = sum(year_counts.values())
        print(f'    Total: Scanned {total_rows:,} rows, loaded {total_kept:,} rows for {len(years_list)} years')

        # Show per-year breakdown if verbose
        if len(years_list) > 1:
            print(f'    Per-year breakdown:')
            for year in sorted(year_counts.keys()):
                if year_counts[year] > 0:
                    print(f'      {year}: {year_counts[year]:,} rows')


def create_indexes(conn, tables, verbose=False):
    """
    Create indexes on commonly queried fields for performance.
    Only creates indexes if the table actually exists.

    Args:
        conn: SQLite database connection
        tables: List of tables that were added
        verbose: Whether to print progress messages
    """
    if verbose:
        print('\nCreating indexes...')

    # Get list of existing tables
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    )
    existing_tables = {row[0] for row in cursor.fetchall()}

    if 'master' in tables and 'master' in existing_tables:
        conn.execute('CREATE INDEX IF NOT EXISTS idx_master_key ON master(mdr_report_key)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_master_date ON master(date_received)')

    if 'device' in tables and 'device' in existing_tables:
        conn.execute('CREATE INDEX IF NOT EXISTS idx_device_key ON device(MDR_REPORT_KEY)')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_device_code ON device(DEVICE_REPORT_PRODUCT_CODE)')

    if 'patient' in tables and 'patient' in existing_tables:
        conn.execute('CREATE INDEX IF NOT EXISTS idx_patient_key ON patient(mdr_report_key)')

    if 'text' in tables and 'text' in existing_tables:
        conn.execute('CREATE INDEX IF NOT EXISTS idx_text_key ON text(MDR_REPORT_KEY)')

    conn.commit()