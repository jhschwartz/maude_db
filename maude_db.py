# maude_db.py - FDA MAUDE Database Interface
# Copyright (C) 2024 Jacob Schwartz, University of Michigan Medical School
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

import pandas as pd
import sqlite3
import os
from datetime import datetime
import requests
import zipfile


class MaudeDatabase:
    """
    Interface to FDA MAUDE database - handles download, parsing, and querying.
    
    Usage:
        # Connect to existing database
        db = MaudeDatabase('maude.db')
        
        # Add data
        db.add_years('2015-2024', tables=['master', 'device'], download=True)
        
        # Query
        results = db.query_device(device_name='thrombectomy')
        
        # Update with latest
        db.update()
    """
    
    base_url = "https://www.accessdata.fda.gov/MAUDE/ftparea"
    
    table_files = {
        'master': 'mdrfoi',
        'device': 'foidev',
        'patient': 'patient',
        'text': 'foitext',
        'problems': 'foidevproblem'
    }

    # Table metadata defining file patterns, availability, and characteristics
    TABLE_METADATA = {
        'master': {
            'file_prefix': 'mdrfoi',
            'pattern_type': 'cumulative',  # Uses mdrfoithru{year}.zip
            'start_year': 1991,
            'current_year_prefix': 'mdrfoi',  # For current year: mdrfoi.zip
            'size_category': 'large',
            'description': 'Master records (adverse event reports)',
            'date_column': 'date_received'  # For filtering cumulative files
        },
        'device': {
            'file_prefix': 'foidev',
            'pattern_type': 'yearly',  # Uses foidev{year}.zip
            'start_year': 1998,
            'current_year_prefix': 'device',  # For current year: device.zip
            'size_category': 'medium',
            'description': 'Device information'
        },
        'text': {
            'file_prefix': 'foitext',
            'pattern_type': 'yearly',  # Uses foitext{year}.zip
            'start_year': 1996,
            'current_year_prefix': 'foitext',  # For current year: foitext.zip
            'size_category': 'medium',
            'description': 'Event narrative text'
        },
        'patient': {
            'file_prefix': 'patient',
            'pattern_type': 'cumulative',  # Uses patientthru{year}.zip
            'start_year': 1996,
            'current_year_prefix': 'patient',  # For current year: patient.zip
            'size_category': 'very_large',
            'description': 'Patient demographics',
            'size_warning': 'Patient data is distributed as a single large file (117MB compressed, 841MB uncompressed). All data will be downloaded even if you only need specific years.',
            'date_column': 'date_of_event'  # For filtering cumulative files
        },
        'problems': {
            'file_prefix': 'foidevproblem',
            'pattern_type': 'yearly',
            'start_year': 2019,  # Approximate - recent years only
            'current_year_prefix': 'foidevproblem',
            'size_category': 'small',
            'description': 'Device problem codes'
        }
    }


    def __init__(self, db_path, verbose=True):
        """
        Initialize connection to MAUDE database.
        Creates new database if doesn't exist, connects to existing if it does.
        
        Args:
            db_path: Path to SQLite database file
            verbose: Whether to print progress messages
        """
        self.db_path = db_path
        self.verbose = verbose
        self.conn = sqlite3.connect(self.db_path)
    

    def __enter__(self):
        """Context manager entry - allows 'with MaudeDatabase() as db:' syntax"""
        return self
    

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - clean up connection"""
        self.conn.close()
    

    def add_years(self, years, tables=None, download=False, strict=False, chunk_size=100000, data_dir='./maude_data', interactive=True):
        """
        Add MAUDE data for specified years to database.

        Args:
            years: Year or years to add. Can be:
                   - Single int: 2024
                   - List: [2020, 2021, 2022]
                   - String range: '2015-2024'
                   - String: 'all', 'latest', 'current'
            tables: List of tables to include (default: all main tables)
            download: Whether to download files from FDA
            strict: If True, raise error on missing files. If False, skip with warning.
            chunk_size: Rows to process at once (for memory efficiency)
            data_dir: Directory containing data files
            interactive: If True, prompt user for validation issues (default: True)
        """
        years_list = self._parse_year_range(years)

        if tables is None:
            tables = ['master', 'device', 'patient', 'text']

        # Validate year/table compatibility
        validation_result = self._validate_year_table_compatibility(years_list, tables)

        # Handle interactive validation
        if interactive and (validation_result['invalid'] or validation_result['warnings']):
            proceed, valid_combinations = self._prompt_user_for_validation_resolution(validation_result)
            if not proceed:
                if self.verbose:
                    print("\nOperation cancelled by user.")
                return
        else:
            # Non-interactive mode: use only valid combinations
            valid_combinations = validation_result['valid']

            # In strict mode, raise error if any invalid combinations
            if strict and validation_result['invalid']:
                invalid_msgs = [f"{t} {y}: {r}" for y, t, r in validation_result['invalid']]
                raise ValueError(f"Invalid year/table combinations:\n" + "\n".join(invalid_msgs))

        # Group valid combinations by year for efficient processing
        years_to_process = {}
        for year, table in valid_combinations:
            if year not in years_to_process:
                years_to_process[year] = []
            years_to_process[year].append(table)

        # Track which tables were actually loaded
        loaded_tables = set()
        current_year = datetime.now().year

        # Process each year
        for year in sorted(years_to_process.keys()):
            tables_for_year = years_to_process[year]

            if self.verbose:
                print(f'\nProcessing year {year}...')

            for table in tables_for_year:
                if download:
                    if not self._download_file(year, table, data_dir):
                        # Try fallback for current year
                        if year == current_year:
                            if self.verbose:
                                print(f'  Current year file not found, trying previous year as fallback...')
                            # Try to download previous year instead
                            if not self._download_file(year - 1, table, data_dir):
                                if strict:
                                    raise FileNotFoundError(f'Could not download {table} for {year} or {year-1}')
                                if self.verbose:
                                    print(f'  Skipping {table} {year} - download failed')
                                continue
                            else:
                                # Successfully downloaded previous year, update year variable for file path
                                year = year - 1
                                if self.verbose:
                                    print(f'  Using {year} data as fallback')
                        else:
                            if strict:
                                raise FileNotFoundError(f'Could not download {table} {year}')
                            if self.verbose:
                                print(f'  Skipping {table} {year} - download failed')
                            continue

                path = self._make_file_path(table, year, data_dir)
                if not path:
                    if strict:
                        raise FileNotFoundError(f'No file found for table={table}, year={year}')
                    if self.verbose:
                        print(f'  Skipping {table} {year} - file not found')
                    continue

                if self.verbose:
                    print(f'  Loading {table}...')

                # Handle cumulative files with year filtering
                if table in self.TABLE_METADATA and self.TABLE_METADATA[table]['pattern_type'] == 'cumulative':
                    self._process_cumulative_file(path, table, year, chunk_size)
                else:
                    self._process_file(path, table, chunk_size)

                loaded_tables.add(table)

        # Only create indexes for tables that were actually loaded
        self._create_indexes(list(loaded_tables))

        if self.verbose:
            print('\nDatabase update complete')


    def _construct_file_url(self, table, year):
        """
        Construct download URL based on table type and year.

        Args:
            table: Table name (e.g., 'master', 'device')
            year: Year as integer

        Returns:
            tuple: (url, filename) or (None, None) if invalid
        """
        if table not in self.TABLE_METADATA:
            return None, None

        metadata = self.TABLE_METADATA[table]
        pattern_type = metadata['pattern_type']
        file_prefix = metadata['file_prefix']
        current_year = datetime.now().year

        # Current year files (no year suffix)
        if year == current_year:
            filename = f"{metadata['current_year_prefix']}.zip"
            url = f"{self.base_url}/{filename}"
            return url, filename

        # Historical files
        if pattern_type == 'yearly':
            # Special case: device table changed naming in 2000
            if table == 'device':
                if year >= 2000:
                    # 2000+: device2020.zip
                    filename = f"device{year}.zip"
                else:
                    # 1998-1999: foidev1999.zip
                    filename = f"{file_prefix}{year}.zip"
            else:
                # Standard yearly pattern: foitext2020.zip
                filename = f"{file_prefix}{year}.zip"

            url = f"{self.base_url}/{filename}"
            return url, filename

        elif pattern_type == 'cumulative':
            # Cumulative pattern: mdrfoithru2020.zip
            filename = f"{file_prefix}thru{year}.zip"
            url = f"{self.base_url}/{filename}"
            return url, filename

        return None, None

    def _parse_year_range(self, year_str):
        """
        Convert year string to list of year integers.

        Args:
            year_str: String like '2015-2024', 'all', 'latest', 'current', or single year '2024'

        Returns:
            List of year integers
        """
        if isinstance(year_str, int):
            return [year_str]

        if isinstance(year_str, list):
            return year_str

        if year_str == 'all':
            return list(range(1991, datetime.now().year + 1))
        elif year_str == 'latest':
            return [datetime.now().year - 1]
        elif year_str == 'current':
            return [datetime.now().year]
        elif '-' in year_str:
            start, end = year_str.split('-')
            return list(range(int(start), int(end) + 1))

        return [int(year_str)]

    def _validate_year_table_compatibility(self, years, tables):
        """
        Validate that requested years are valid for requested tables.

        Args:
            years: List of year integers
            tables: List of table names

        Returns:
            dict: {
                'valid': [(year, table), ...],
                'invalid': [(year, table, reason), ...],
                'warnings': [(year, table, warning_msg), ...]
            }
        """
        valid = []
        invalid = []
        warnings = []
        current_year = datetime.now().year

        for year in years:
            for table in tables:
                if table not in self.TABLE_METADATA:
                    invalid.append((year, table, f"Unknown table '{table}'"))
                    continue

                metadata = self.TABLE_METADATA[table]
                start_year = metadata['start_year']

                # Check if year is too old
                if year < start_year:
                    invalid.append((
                        year,
                        table,
                        f"{metadata['description']} only available from {start_year} onwards"
                    ))
                    continue

                # Check if year is in the future
                if year > current_year:
                    invalid.append((
                        year,
                        table,
                        f"Year {year} is in the future"
                    ))
                    continue

                # Check if current year file might not exist yet (year transition)
                if year == current_year:
                    # Check if it's early in the year (before February)
                    if datetime.now().month < 2:
                        warnings.append((
                            year,
                            table,
                            f"Current year ({current_year}) files may not be available yet. Will attempt download with fallback."
                        ))

                # Add size warnings for patient table
                if table == 'patient' and 'size_warning' in metadata:
                    # Only warn once per patient table request, not per year
                    patient_warning = (
                        year,
                        table,
                        metadata['size_warning']
                    )
                    if patient_warning not in warnings:
                        warnings.append(patient_warning)

                valid.append((year, table))

        return {
            'valid': valid,
            'invalid': invalid,
            'warnings': warnings
        }

    def _prompt_user_for_validation_resolution(self, validation_result):
        """
        Interactively prompt user to resolve validation issues.

        Args:
            validation_result: Output from _validate_year_table_compatibility

        Returns:
            tuple: (proceed: bool, filtered_valid: list of (year, table) tuples)
        """
        invalid = validation_result['invalid']
        warnings = validation_result['warnings']
        valid = validation_result['valid']

        if not invalid and not warnings:
            return True, valid

        print("\n" + "="*60)
        print("DATA AVAILABILITY VALIDATION")
        print("="*60)

        # Show invalid combinations
        if invalid:
            print("\nINVALID YEAR/TABLE COMBINATIONS:")
            print("-" * 60)
            for year, table, reason in invalid:
                print(f"  X {table} {year}: {reason}")

            print("\nThese combinations will be skipped.")
            print("\nOptions:")
            print("  1. Continue with valid combinations only")
            print("  2. Abort and adjust your request")

            while True:
                choice = input("\nYour choice (1 or 2): ").strip()
                if choice == '1':
                    break
                elif choice == '2':
                    return False, []
                else:
                    print("  Invalid choice. Please enter 1 or 2.")

        # Show warnings
        if warnings:
            print("\nWARNINGS:")
            print("-" * 60)

            # Group warnings by type
            patient_warnings = [(y, t, w) for y, t, w in warnings if 'patient' in t.lower() and 'large file' in w.lower()]
            other_warnings = [(y, t, w) for y, t, w in warnings if not ('patient' in t.lower() and 'large file' in w.lower())]

            # Patient size warnings
            if patient_warnings:
                print("\nPATIENT TABLE SIZE WARNING:")
                print(patient_warnings[0][2])  # Show warning message
                print("\nDo you want to proceed with patient table download?")

                while True:
                    choice = input("Proceed? (y/n): ").strip().lower()
                    if choice in ['y', 'yes']:
                        break
                    elif choice in ['n', 'no']:
                        # Remove patient from valid list
                        valid = [(y, t) for y, t in valid if t != 'patient']
                        print("  Patient table removed from download list.")
                        break
                    else:
                        print("  Invalid choice. Please enter y or n.")

            # Other warnings
            if other_warnings:
                for year, table, warning in other_warnings:
                    print(f"  ! {table} {year}: {warning}")
                print("\nProceed despite warnings?")

                while True:
                    choice = input("(y/n): ").strip().lower()
                    if choice in ['y', 'yes']:
                        break
                    elif choice in ['n', 'no']:
                        return False, []
                    else:
                        print("  Invalid choice. Please enter y or n.")

        return True, valid


    def _make_file_path(self, table, year, data_dir='./maude_data'):
        """
        Create a path for the MAUDE datafile, checking all possible naming patterns.

        Args:
            table: Table name (e.g., 'master', 'device')
            year: Year as integer
            data_dir: Directory containing data files

        Returns:
            path if it exists
            Otherwise returns False
        """
        if table not in self.TABLE_METADATA:
            return False

        metadata = self.TABLE_METADATA[table]
        file_prefix = metadata['file_prefix']
        pattern_type = metadata['pattern_type']
        current_year = datetime.now().year

        if not os.path.exists(data_dir):
            return False

        files_in_dir = os.listdir(data_dir)

        # Patterns to check (both lowercase and uppercase)
        patterns = []

        if year == current_year:
            # Current year pattern: device.txt or DEVICE.txt
            current_prefix = metadata['current_year_prefix']
            patterns.extend([
                f"{current_prefix}.txt",
                f"{current_prefix.upper()}.txt"
            ])

        if pattern_type == 'yearly':
            # Special case: device table changed naming in 2000
            if table == 'device':
                if year >= 2000:
                    # 2000+: device2020.txt or DEVICE2020.txt
                    patterns.extend([
                        f"device{year}.txt",
                        f"DEVICE{year}.txt"
                    ])
                else:
                    # 1998-1999: foidev1999.txt or FOIDEV1999.txt
                    patterns.extend([
                        f"{file_prefix}{year}.txt",
                        f"{file_prefix.upper()}{year}.txt"
                    ])
            else:
                # Yearly pattern: foitext2020.txt
                patterns.extend([
                    f"{file_prefix}{year}.txt",
                    f"{file_prefix.upper()}{year}.txt"
                ])

        elif pattern_type == 'cumulative':
            # Cumulative pattern: mdrfoithru2020.txt
            patterns.extend([
                f"{file_prefix}thru{year}.txt",
                f"{file_prefix.upper()}thru{year}.txt",
                f"{file_prefix.upper()}THRU{year}.txt",
                f"{file_prefix}Thru{year}.txt"
            ])

        # Check each pattern
        for pattern in patterns:
            if pattern in files_in_dir:
                return f"{data_dir}/{pattern}"

        return False


    def _download_file(self, year, table, data_dir='./maude_data'):
        """
        Download and extract a MAUDE file from FDA.

        Args:
            year: Year to download
            table: Table name (e.g., 'master', 'device')
            data_dir: Directory to save files

        Returns:
            True if successful, False otherwise
        """
        os.makedirs(data_dir, exist_ok=True)

        url, filename = self._construct_file_url(table, year)
        if not url:
            if self.verbose:
                print(f'  Cannot construct URL for {table} year {year}')
            return False

        zip_path = f"{data_dir}/{filename}"

        if os.path.exists(zip_path):
            if self.verbose:
                print(f'  Using cached {filename}')
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(data_dir)
                return True
            except:
                os.remove(zip_path)

        try:
            if self.verbose:
                print(f'  Downloading {filename}...')

            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()

            with open(zip_path, 'wb') as f:
                f.write(response.content)

            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(data_dir)

            return True

        except Exception as e:
            if self.verbose:
                print(f'  Error downloading {year}: {e}')
            return False


    def _check_file_exists(self, year, file_prefix):
        """
        Check if a file exists on FDA server without downloading.
        
        Args:
            year: Year to check
            file_prefix: File type to check
        
        Returns:
            True if file exists (status 200), False otherwise
        """
        url = f"{self.base_url}/{file_prefix}{year}.zip"
        
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            response = requests.head(url, headers=headers, timeout=5)
            return response.status_code == 200
        except:
            return False


    def _process_file(self, filepath, table_name, chunk_size):
        """
        Read MAUDE text file and insert into SQLite database.

        Args:
            filepath: Path to pipe-delimited text file
            table_name: Name of table to insert into
            chunk_size: Number of rows to process at once
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
            chunk.to_sql(table_name, self.conn, if_exists='append', index=False)
            total_rows += len(chunk)

            if self.verbose and i % 10 == 0 and i > 0:
                print(f'    Processed {total_rows:,} rows...')

        if self.verbose:
            print(f'    Total: {total_rows:,} rows')

    def _process_cumulative_file(self, filepath, table_name, year, chunk_size):
        """
        Read cumulative MAUDE file and insert only specified year into database.

        For tables like master and patient that are distributed as cumulative files,
        this filters to only include records from the specified year.

        Args:
            filepath: Path to pipe-delimited text file
            table_name: Name of table to insert into
            year: Year to filter for
            chunk_size: Number of rows to process at once
        """
        total_rows = 0
        filtered_rows = 0

        # Get date column from metadata
        if table_name not in self.TABLE_METADATA:
            # Fallback to regular processing if we don't know the metadata
            return self._process_file(filepath, table_name, chunk_size)

        metadata = self.TABLE_METADATA[table_name]

        if 'date_column' not in metadata:
            # No date column defined, process entire file
            return self._process_file(filepath, table_name, chunk_size)

        date_col = metadata['date_column']

        if self.verbose:
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
                if self.verbose and i == 0:
                    print(f'    Warning: Date column {date_col} not found, loading all data')
                chunk_filtered = chunk

            if len(chunk_filtered) > 0:
                chunk_filtered.to_sql(table_name, self.conn, if_exists='append', index=False)
                filtered_rows += len(chunk_filtered)

            if self.verbose and i % 10 == 0 and i > 0:
                print(f'    Scanned {total_rows:,} rows, kept {filtered_rows:,}...')

        if self.verbose:
            print(f'    Total: Scanned {total_rows:,} rows, loaded {filtered_rows:,} rows for year {year}')


    def _create_indexes(self, tables):
        """
        Create indexes on commonly queried fields for performance.
        Only creates indexes if the table actually exists.
        
        Args:
            tables: List of tables that were added
        """
        if self.verbose:
            print('\nCreating indexes...')
        
        # Get list of existing tables
        cursor = self.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        existing_tables = {row[0] for row in cursor.fetchall()}
        
        if 'master' in tables and 'master' in existing_tables:
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_master_key ON master(mdr_report_key)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_master_date ON master(date_received)')
        
        if 'device' in tables and 'device' in existing_tables:
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_device_key ON device(MDR_REPORT_KEY)')
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_device_code ON device(DEVICE_REPORT_PRODUCT_CODE)')
        
        if 'patient' in tables and 'patient' in existing_tables:
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_patient_key ON patient(mdr_report_key)')
        
        if 'text' in tables and 'text' in existing_tables:
            self.conn.execute('CREATE INDEX IF NOT EXISTS idx_text_key ON text(MDR_REPORT_KEY)')
        
        self.conn.commit()


    def update(self):
        """
        Add the most recent available year to database.
        Checks what years are already in database and adds newest if missing.
        """
        latest = self._get_latest_available_year()
        existing = self._get_years_in_db()
        
        if latest not in existing:
            if self.verbose:
                print(f'Updating with {latest} data...')
            self.add_years([latest], download=True)
        else:
            if self.verbose:
                print('Database is up to date')


    def _get_latest_available_year(self):
        """
        Find the most recent year available on FDA server.
        
        Returns:
            Most recent year as integer
        """
        current_year = datetime.now().year
        
        for year in range(current_year, 1990, -1):
            if self._check_file_exists(year, 'mdrfoi'):
                return year
        
        return current_year - 1


    def _get_years_in_db(self):
        """
        Get list of years currently in the database.
        
        Returns:
            List of years (as integers)
        """
        try:
            df = pd.read_sql_query(
                "SELECT DISTINCT strftime('%Y', date_received) as year FROM master",
                self.conn
            )
            return [int(y) for y in df['year'].tolist() if y]
        except:
            return []


    def query(self, sql, params=None):
        """
        Execute raw SQL query and return results as DataFrame.
        
        Args:
            sql: SQL query string
            params: Optional parameters for query (for safety)
        
        Returns:
            pandas DataFrame with results
        """
        return pd.read_sql_query(sql, self.conn, params=params)


    def query_device(self, device_name=None, product_code=None, 
                     start_date=None, end_date=None):
        """
        Query device events with optional filters.
        
        Args:
            device_name: Filter by generic_name or brand_name (partial match)
            product_code: Filter by exact product code
            start_date: Only events on/after this date
            end_date: Only events on/before this date
        
        Returns:
            DataFrame with matching records from master + device tables
        """
        conditions = []
        params = {}
        
        if device_name:
            conditions.append("(d.GENERIC_NAME LIKE :device OR d.BRAND_NAME LIKE :device)")
            params['device'] = f'%{device_name}%'

        if product_code:
            conditions.append("d.DEVICE_REPORT_PRODUCT_CODE = :code")
            params['code'] = product_code
        
        if start_date:
            conditions.append("m.date_received >= :start")
            params['start'] = start_date
        
        if end_date:
            conditions.append("m.date_received <= :end")
            params['end'] = end_date
        
        where = " AND ".join(conditions) if conditions else "1=1"
        
        sql = f"""
            SELECT m.*, d.*
            FROM master m
            JOIN device d ON m.mdr_report_key = d.mdr_report_key
            WHERE {where}
        """
        
        return pd.read_sql_query(sql, self.conn, params=params)


    def get_trends_by_year(self, product_code=None, device_name=None):
        """
        Get yearly event counts and breakdown by event type.
        
        Args:
            product_code: Optional filter by product code
            device_name: Optional filter by device name
        
        Returns:
            DataFrame with columns: year, event_count, deaths, injuries, malfunctions
        """
        condition = "1=1"
        params = {}
        
        if product_code:
            condition = "d.DEVICE_REPORT_PRODUCT_CODE = :code"
            params['code'] = product_code
        elif device_name:
            condition = "(d.GENERIC_NAME LIKE :name OR d.BRAND_NAME LIKE :name)"
            params['name'] = f'%{device_name}%'
        
        sql = f"""
            SELECT 
                strftime('%Y', m.date_received) as year,
                COUNT(*) as event_count,
                SUM(CASE WHEN m.event_type LIKE '%Death%' THEN 1 ELSE 0 END) as deaths,
                SUM(CASE WHEN m.event_type LIKE '%Injury%' THEN 1 ELSE 0 END) as injuries,
                SUM(CASE WHEN m.event_type LIKE '%Malfunction%' THEN 1 ELSE 0 END) as malfunctions
            FROM master m
            JOIN device d ON m.mdr_report_key = d.mdr_report_key
            WHERE {condition}
            GROUP BY year
            ORDER BY year
        """
        
        return pd.read_sql_query(sql, self.conn, params=params)


    def get_narratives(self, mdr_report_keys):
        """
        Get text narratives for specific report keys.
        
        Args:
            mdr_report_keys: List of MDR report keys
        
        Returns:
            DataFrame with mdr_report_key and narrative text
        """
        placeholders = ','.join('?' * len(mdr_report_keys))

        sql = f"""
            SELECT MDR_REPORT_KEY, FOI_TEXT
            FROM text
            WHERE MDR_REPORT_KEY IN ({placeholders})
        """
        
        return pd.read_sql_query(sql, self.conn, params=mdr_report_keys)


    def export_subset(self, output_file, **filters):
        """
        Export filtered data to CSV file.
        
        Args:
            output_file: Path for output CSV
            **filters: Keyword arguments passed to query_device()
        """
        df = self.query_device(**filters)
        df.to_csv(output_file, index=False)
        
        if self.verbose:
            print(f'Exported {len(df):,} records to {output_file}')


    def info(self):
        """
        Print summary statistics about the database.
        Displays: tables present, record counts, date range
        """
        print(f"\nDatabase: {self.db_path}")
        print("=" * 60)
        
        tables = pd.read_sql_query(
            "SELECT name FROM sqlite_master WHERE type='table'",
            self.conn
        )['name'].tolist()
        
        if not tables:
            print("Database is empty")
            return
        
        for table in tables:
            count = pd.read_sql_query(
                f"SELECT COUNT(*) as count FROM {table}",
                self.conn
            )['count'][0]
            print(f"{table:15} {count:,} records")
        
        if 'master' in tables:
            date_info = pd.read_sql_query(
                "SELECT MIN(date_received) as first, MAX(date_received) as last FROM master",
                self.conn
            ).iloc[0]
            print(f"\nDate range: {date_info['first']} to {date_info['last']}")
        
        db_size = os.path.getsize(self.db_path) / (1024**3)
        print(f"Database size: {db_size:.2f} GB")
    

    def close(self):
        """Close database connection."""
        self.conn.close()