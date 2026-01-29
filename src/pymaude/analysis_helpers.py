"""
Analysis helper functions for MAUDE data.

These functions operate on pandas DataFrames returned by MaudeDatabase query methods.
They provide common analysis patterns to reduce boilerplate in notebooks.
"""

import pandas as pd
import matplotlib.pyplot as plt
from typing import Dict, List, Optional, Union
import os


# ==================== Internal Helper Functions ====================

def _batched_query_by_keys(db, table, columns, mdr_keys, batch_size=900):
    """
    Execute batched SQL queries to avoid SQLite variable limit.

    SQLite has a limit on the number of variables in a query (typically 999).
    This function splits large key lists into batches and concatenates results.

    Args:
        db: MaudeDatabase instance
        table: Table name to query
        columns: Column specification (e.g., '*' or 'MDR_REPORT_KEY, FOI_TEXT')
        mdr_keys: List of MDR_REPORT_KEY values to query
        batch_size: Maximum keys per batch (default 900, under SQLite's 999 limit)

    Returns:
        DataFrame with query results from all batches concatenated
    """
    if not mdr_keys:
        return pd.DataFrame()

    result_dfs = []
    for i in range(0, len(mdr_keys), batch_size):
        batch_keys = mdr_keys[i:i + batch_size]
        placeholders = ','.join(['?'] * len(batch_keys))
        batch_df = pd.read_sql_query(f"""
            SELECT {columns}
            FROM {table}
            WHERE MDR_REPORT_KEY IN ({placeholders})
        """, db.conn, params=batch_keys)
        result_dfs.append(batch_df)

    return pd.concat(result_dfs, ignore_index=True) if result_dfs else pd.DataFrame()


# ==================== Existing Helper Methods (Moved from database.py) ====================

def get_narratives_for(db, results_df):
    """
    Get narratives for a query result DataFrame.

    Convenience method that extracts MDR_REPORT_KEYs from a DataFrame
    and retrieves their narratives. Useful for chaining after query_device().

    Args:
        db: MaudeDatabase instance
        results_df: DataFrame containing MDR_REPORT_KEY column
                   (typically from query_device() or similar)

    Returns:
        DataFrame with mdr_report_key and narrative text

    Example:
        results = db.query_device(device_name='thrombectomy')
        narratives = get_narratives_for(db, results)
    """
    if 'MDR_REPORT_KEY' not in results_df.columns:
        raise ValueError("DataFrame must contain 'MDR_REPORT_KEY' column")

    keys = results_df['MDR_REPORT_KEY'].tolist()
    return db.get_narratives(keys)


def trends_for(results_df):
    """
    Get yearly trends for a query result DataFrame.

    Analyzes the provided DataFrame to compute yearly event counts
    and breakdowns by event type (deaths, injuries, malfunctions).

    Args:
        results_df: DataFrame with DATE_RECEIVED and EVENT_TYPE columns
                   (typically from query_device())

    Returns:
        DataFrame with columns: year, event_count, deaths, injuries, malfunctions

    Example:
        results = db.query_device(device_name='pacemaker')
        trends = trends_for(results)
    """
    required_cols = ['DATE_RECEIVED', 'EVENT_TYPE']
    missing = [col for col in required_cols if col not in results_df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    # Create a temporary copy with year extracted
    # Handle duplicate columns (e.g., when joining master and device tables)
    df = results_df.copy()
    # Get the first DATE_RECEIVED column if there are duplicates
    date_received = df['DATE_RECEIVED']
    if isinstance(date_received, pd.DataFrame):
        # Multiple DATE_RECEIVED columns - use the first one (from master table)
        date_received = date_received.iloc[:, 0]
    df['year'] = pd.to_datetime(date_received).dt.year

    # Get EVENT_TYPE column, handling duplicates if they exist
    event_type_col = df['EVENT_TYPE']
    if isinstance(event_type_col, pd.DataFrame):
        # Multiple EVENT_TYPE columns - use the first one (from master table)
        event_type_col = event_type_col.iloc[:, 0]
    df['event_type_clean'] = event_type_col

    # Aggregate by year
    # FDA uses abbreviations: D=Death, IN=Injury, M=Malfunction
    trends = df.groupby('year').agg(
        event_count=('year', 'size'),
        deaths=('event_type_clean', lambda x: x.str.contains(r'\bD\b|Death', case=False, na=False, regex=True).sum()),
        injuries=('event_type_clean', lambda x: x.str.contains(r'\bIN\b|Injury', case=False, na=False, regex=True).sum()),
        malfunctions=('event_type_clean', lambda x: x.str.contains(r'\bM\b|Malfunction', case=False, na=False, regex=True).sum())
    ).reset_index()

    return trends


def event_type_breakdown_for(results_df):
    """
    Get event type breakdown for a query result DataFrame.

    Provides summary statistics of event types (deaths, injuries, malfunctions)
    in the provided DataFrame. Counts unique events (MDR_REPORT_KEY) to handle
    cases where multiple devices may be associated with a single event.

    Args:
        results_df: DataFrame with EVENT_TYPE and MDR_REPORT_KEY columns
                   (typically from query_device())

    Returns:
        dict with counts: {
            'total': int,
            'deaths': int,
            'injuries': int,
            'malfunctions': int,
            'other': int
        }

    Example:
        results = db.query_device(device_name='thrombectomy')
        breakdown = event_type_breakdown_for(results)
        print(f"Deaths: {breakdown['deaths']}")
    """
    if 'EVENT_TYPE' not in results_df.columns:
        raise ValueError("DataFrame must contain 'EVENT_TYPE' column")

    # Count unique events (MDR_REPORT_KEY) to avoid double-counting multi-device events
    if 'MDR_REPORT_KEY' in results_df.columns:
        # Get unique MDR_REPORT_KEYs with their EVENT_TYPE
        mdr_key = results_df['MDR_REPORT_KEY']
        if isinstance(mdr_key, pd.DataFrame):
            mdr_key = mdr_key.iloc[:, 0]

        event_type = results_df['EVENT_TYPE']
        if isinstance(event_type, pd.DataFrame):
            event_type = event_type.iloc[:, 0]

        # Create a deduplicated DataFrame
        unique_df = pd.DataFrame({
            'MDR_REPORT_KEY': mdr_key,
            'EVENT_TYPE': event_type
        }).drop_duplicates(subset=['MDR_REPORT_KEY'])

        total = len(unique_df)
        event_type = unique_df['EVENT_TYPE'].fillna('')
    else:
        # Fallback: count all rows if MDR_REPORT_KEY not available
        total = len(results_df)
        event_type = results_df['EVENT_TYPE']
        if isinstance(event_type, pd.DataFrame):
            event_type = event_type.iloc[:, 0]
        event_type = event_type.fillna('')

    # FDA uses abbreviations: D=Death, IN=Injury, M=Malfunction
    # Also check for full words for backwards compatibility
    deaths = event_type.str.contains(r'\bD\b|Death', case=False, regex=True).sum()
    injuries = event_type.str.contains(r'\bIN\b|Injury', case=False, regex=True).sum()
    malfunctions = event_type.str.contains(r'\bM\b|Malfunction', case=False, regex=True).sum()

    # Events can have multiple types, so other is approximate
    other = total - max(deaths, injuries, malfunctions)

    return {
        'total': total,
        'deaths': int(deaths),
        'injuries': int(injuries),
        'malfunctions': int(malfunctions),
        'other': max(0, int(other))
    }


def top_manufacturers_for(results_df, n=10):
    """
    Get top manufacturers from a query result DataFrame.

    Args:
        results_df: DataFrame with MANUFACTURER_D_NAME column
                   (typically from query_device())
        n: Number of top manufacturers to return (default: 10)

    Returns:
        DataFrame with columns: manufacturer, event_count
        Sorted by event_count descending

    Example:
        results = db.query_device(device_name='pacemaker')
        top_mfg = top_manufacturers_for(results, n=5)
    """
    if 'MANUFACTURER_D_NAME' not in results_df.columns:
        raise ValueError("DataFrame must contain 'MANUFACTURER_D_NAME' column")

    counts = results_df['MANUFACTURER_D_NAME'].value_counts().head(n)
    return pd.DataFrame({
        'manufacturer': counts.index,
        'event_count': counts.values
    })


def date_range_summary_for(results_df):
    """
    Get date range summary for a query result DataFrame.

    Args:
        results_df: DataFrame with DATE_RECEIVED column
                   (typically from query_device())

    Returns:
        dict with: {
            'first_date': str,
            'last_date': str,
            'total_days': int,
            'total_records': int
        }

    Example:
        results = db.query_device(device_name='thrombectomy')
        summary = date_range_summary_for(results)
        print(f"Data spans {summary['total_days']} days")
    """
    if 'DATE_RECEIVED' not in results_df.columns:
        raise ValueError("DataFrame must contain 'DATE_RECEIVED' column")

    # Handle duplicate DATE_RECEIVED columns (e.g., from joining master and device tables)
    date_received = results_df['DATE_RECEIVED']
    if isinstance(date_received, pd.DataFrame):
        # Multiple DATE_RECEIVED columns - use the first one
        date_received = date_received.iloc[:, 0]

    dates = pd.to_datetime(date_received)
    first = dates.min()
    last = dates.max()

    return {
        'first_date': str(first.date()) if pd.notna(first) else None,
        'last_date': str(last.date()) if pd.notna(last) else None,
        'total_days': (last - first).days if pd.notna(first) and pd.notna(last) else 0,
        'total_records': len(results_df)
    }


# ==================== New Multi-Device Methods ====================

def enrich_with_problems(db, results_df):
    """
    Join device problem codes to query results.

    Args:
        db: MaudeDatabase instance
        results_df: DataFrame from query_device() or similar

    Returns:
        DataFrame with problems table columns joined

    Raises:
        ValueError: If problems table not loaded

    Example:
        results = db.query_device(device_name='thrombectomy')
        enriched = enrich_with_problems(db, results)
        print(enriched[['MDR_REPORT_KEY', 'DEVICE_PROBLEM_CODE']])
    """
    # Check MDR_REPORT_KEY column
    if 'MDR_REPORT_KEY' not in results_df.columns:
        raise ValueError("DataFrame must contain 'MDR_REPORT_KEY' column")

    # Check problems table exists (STRICT)
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='problems'",
        db.conn
    )['name'].tolist()

    if 'problems' not in tables:
        raise ValueError(
            "Problems table not loaded. Load with:\n"
            "  db.add_years(years, tables=['problems'], download=True)\n"
            "Note: problems table only available from 2019 onwards"
        )

    mdr_keys = results_df['MDR_REPORT_KEY'].unique().tolist()

    if not mdr_keys:
        return results_df

    # Use batched query to avoid SQLite variable limit
    problems = _batched_query_by_keys(
        db, 'problems',
        'MDR_REPORT_KEY, DEVICE_SEQUENCE_NUMBER, DEVICE_PROBLEM_CODE',
        mdr_keys
    )

    if db.verbose:
        print(f"Joined {len(problems)} device problem entries")

    # Left join to preserve all original rows
    enriched = results_df.merge(
        problems,
        on='MDR_REPORT_KEY',
        how='left'
    )

    return enriched


def enrich_with_patient_data(db, results_df):
    """
    Join patient outcome data to query results.

    Args:
        db: MaudeDatabase instance
        results_df: DataFrame from query_device() or similar

    Returns:
        DataFrame with patient table columns joined, including parsed outcome_codes

    Raises:
        ValueError: If patient table not loaded

    Example:
        results = db.query_device(device_name='stent')
        enriched = enrich_with_patient_data(db, results)
        # Check outcome codes
        deaths = enriched[enriched['outcome_codes'].apply(lambda x: 'D' in x if x else False)]
    """
    # Check MDR_REPORT_KEY column
    if 'MDR_REPORT_KEY' not in results_df.columns:
        raise ValueError("DataFrame must contain 'MDR_REPORT_KEY' column")

    # Check patient table exists (STRICT)
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='patient'",
        db.conn
    )['name'].tolist()

    if 'patient' not in tables:
        raise ValueError(
            "Patient table not loaded. Load with:\n"
            "  db.add_years(years, tables=['patient'], download=True)"
        )

    mdr_keys = results_df['MDR_REPORT_KEY'].unique().tolist()

    if not mdr_keys:
        return results_df

    # Use batched query to avoid SQLite variable limit
    patient = _batched_query_by_keys(db, 'patient', '*', mdr_keys)

    if db.verbose:
        print(f"Joined {len(patient)} patient records")

    # Parse outcome codes from semicolon-separated strings
    def parse_outcomes(outcome_str):
        if pd.isna(outcome_str):
            return []
        return [code.strip() for code in str(outcome_str).split(';') if code.strip()]

    if len(patient) > 0 and 'SEQUENCE_NUMBER_OUTCOME' in patient.columns:
        patient['outcome_codes'] = patient['SEQUENCE_NUMBER_OUTCOME'].apply(parse_outcomes)

    # Left join to preserve all original rows
    enriched = results_df.merge(
        patient,
        on='MDR_REPORT_KEY',
        how='left'
    )

    return enriched


def enrich_with_narratives(db, results_df):
    """
    Join event narrative text to query results.

    Args:
        db: MaudeDatabase instance
        results_df: DataFrame from query_device() or similar

    Returns:
        DataFrame with narrative column (FOI_TEXT) joined

    Raises:
        ValueError: If text table not loaded

    Example:
        results = db.query_device(device_name='catheter', start_date='2023-01-01')
        with_text = enrich_with_narratives(db, results)
        # Review serious events
        serious = with_text[with_text['EVENT_TYPE'].str.contains('Death|Injury', na=False)]
        for idx, row in serious.head(5).iterrows():
            print(f"\\nReport {row['MDR_REPORT_KEY']}:")
            print(row['FOI_TEXT'][:500])
    """
    # Check MDR_REPORT_KEY column
    if 'MDR_REPORT_KEY' not in results_df.columns:
        raise ValueError("DataFrame must contain 'MDR_REPORT_KEY' column")

    # Check text table exists (STRICT)
    tables = pd.read_sql_query(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='text'",
        db.conn
    )['name'].tolist()

    if 'text' not in tables:
        raise ValueError(
            "Text table not loaded. Load with:\n"
            "  db.add_years(years, tables=['text'], download=True)"
        )

    mdr_keys = results_df['MDR_REPORT_KEY'].unique().tolist()

    if not mdr_keys:
        return results_df

    # Use batched query to avoid SQLite variable limit
    text = _batched_query_by_keys(db, 'text', 'MDR_REPORT_KEY, FOI_TEXT', mdr_keys)

    if db.verbose:
        print(f"Joined {len(text)} narrative texts")

    # Left join to preserve all original rows
    enriched = results_df.merge(
        text,
        on='MDR_REPORT_KEY',
        how='left'
    )

    return enriched


def summarize_by_brand(results_df, group_column='search_group', include_temporal=True):
    """
    Generate summary statistics by device brand or search group.

    Args:
        results_df: DataFrame from search_by_device_names() or query_device()
        group_column: Column to group by (default: 'search_group')
        include_temporal: Include yearly breakdowns (default: True)

    Returns:
        Dict with:
            'counts': Total reports per group (dict)
            'event_types': Event type breakdown per group (DataFrame)
            'date_range': First/last report dates per group (DataFrame)
            'temporal': Yearly counts per group (DataFrame, if include_temporal=True)

    Example:
        # Grouped search
        results = db.search_by_device_names({'g1': 'argon', 'g2': 'penumbra'})
        summary = summarize_by_brand(results)  # Uses search_group column
        print(summary['counts'])
        print(summary['temporal'])

        # Custom grouping column (e.g., after brand standardization)
        summary = summarize_by_brand(results, group_column='standard_brand')

    Author: Jacob Schwartz <jaschwa@umich.edu>
    Copyright: 2026, GNU GPL v3
    """
    if group_column not in results_df.columns:
        raise ValueError(f"DataFrame must contain '{group_column}' column")

    summary = {}

    # Basic counts
    summary['counts'] = results_df[group_column].value_counts().to_dict()

    # Event type breakdown
    if 'EVENT_TYPE' in results_df.columns:
        summary['event_types'] = results_df.groupby(
            [group_column, 'EVENT_TYPE']
        ).size().unstack(fill_value=0)

    # Date range
    if 'DATE_RECEIVED' in results_df.columns:
        summary['date_range'] = results_df.groupby(group_column)['DATE_RECEIVED'].agg([
            ('first_report', 'min'),
            ('last_report', 'max'),
            ('total_reports', 'count')
        ])

    # Temporal trends
    if include_temporal and 'DATE_RECEIVED' in results_df.columns:
        df_copy = results_df.copy()
        df_copy['year'] = pd.to_datetime(df_copy['DATE_RECEIVED']).dt.year
        summary['temporal'] = df_copy.groupby(
            [group_column, 'year']
        ).size().unstack(fill_value=0)

    return summary


def _combine_device_names_search_groups(results_df, group_names, group_var='search_group', preserve_groups=True):
    """
    Combine dataframes of individual search group reuslts.
    
    E.g., maybe you want to combine the following groups:
        - "cleaner_15", corresponding to the device model "Argon Medical Cleaner 15"
        - "cleaner_xt", corresponding to the device model "Argon Medical Cleaner XT"
        - "cleaner_unspecified", corresponding to results of either device, but which 
          did not meet search criteria for specifically models 15 or XT
    If your results were in the var `results`, you would call this function as:
        cleaner_all = combine_device_names_search_groups(
            results, 
            ['cleaner_15', 'cleaner_xt', 'cleaner_unspecified']
        )
    The returned dataframe would be only the events corresponding to the 3 prior groups.
    

    Args:
        results_df: DataFrame of device events with a column defining prior group assignment
        group_names: List[str] listing names of groups 
        group_var: str, the column name that defines groups in results_df
        preserve_groups: bool, whether the prior group column should be kept in the results
    Returns: 
        DataFrame of device events of the new group only

    
    Example: 
        >> results
            search_group    MDR_REPORT_KEY    ...
        0   group-A         abcdefg-1
        1   group-B         abcdefg-2 
        2   group-B         abcdefg-3
        3   group-B         abcdefg-4
        4   group-C         abcdefg-5
        5   group-D         abcdefg-6
        6   group-D         abcdefg-7
        7   group-D         abcdefg-8
        ...
        >> results_2 = combine_device_names_search_groups(results, ['group-A', 'group-B', 'group-C'], preserve_groups=True)
        >> results_2
            search_group    MDR_REPORT_KEY    ...
        0   group-A         abcdefg-1
        1   group-B         abcdefg-2 
        2   group-B         abcdefg-3
        3   group-B         abcdefg-4
        4   group-C         abcdefg-5
        ...
        >> results_3 = combine_device_names_search_groups(results, ['group-A', 'group-B', 'group-C'], preserve_groups=False)
        >> results_3
            MDR_REPORT_KEY    ...
        0   abcdefg-1
        1   abcdefg-2 
        2   abcdefg-3
        3   abcdefg-4
        4   abcdefg-5
        5   abcdefg-6
        6   abcdefg-7
        7   abcdefg-8
    """
    
    desired_groups = []
    for group in group_names:
        df = results_df[results_df[group_var] == group]
        desired_groups.append(df)
    
    new_df = pd.concat(desired_groups)

    if not preserve_groups:
        new_df = new_df.drop(columns=[group_var])
    
    return new_df


def remap_device_groups(results_df, new_group_mapping, group_var='search_group', new_group_column=None, allow_unspecified=False):
    """
    Remaps the groupings within a DataFrame of device events.

    Args:
        results_df: DataFrame of device events with a column defining prior group assignment
        new_group_mapping: dict, mapping of new group names to old group names. Keys are str which
                            correspond to new group names. Values are each either list[str] (combine
                            1 or more prior groups) or str (rename 1 prior group).
        group_var: str, the name of the column in results_df which contains prior group assignments
        new_group_column: str, the name of the column to use to put the new group names in the df. If
                            None (default), then the group_var column will be overwritten with the new
                            groups.
        allow_unspecified: bool, whether we allow some prior groups to be left out of the new mapping.
                            If False (default), a ValueError is raised if any prior groups are not
                            remapped. If True, unmapped groups pass through unchanged (retaining their
                            original group name in the target column).

    Returns:
        DataFrame of device events with the new groupings.

    Raises:
        ValueError: If group_var column is missing from results_df.
        ValueError: If a prior group is assigned to more than one new group.
        ValueError: If allow_unspecified is False and there are unmapped groups.

    Example:
        >> results
            search_group    MDR_REPORT_KEY    ...
        0   group-A         abcdefg-1
        1   group-B         abcdefg-2
        2   group-B         abcdefg-3
        3   group-B         abcdefg-4
        4   group-C         abcdefg-5
        5   group-D         abcdefg-6
        6   group-D         abcdefg-7
        7   group-D         abcdefg-8
        ...
        >> results = remap_device_groups(results, new_group_mapping={
            'group-I': 'group-A',
            'group-II': ['group-B', 'group-C', 'group-D']
        })
        >> results
            search_group    MDR_REPORT_KEY    ...
        0   group-I         abcdefg-1
        1   group-II        abcdefg-2
        2   group-II        abcdefg-3
        3   group-II        abcdefg-4
        4   group-II        abcdefg-5
        5   group-II        abcdefg-6
        6   group-II        abcdefg-7
        7   group-II        abcdefg-8
        ...
    """
    if group_var not in results_df.columns:
        raise ValueError(f"DataFrame must contain '{group_var}' column")

    # Build reverse mapping (old_group -> new_group) and collect old group names
    old_to_new = {}
    for new_group, old_groups in new_group_mapping.items():
        if isinstance(old_groups, str):
            old_to_new[old_groups] = new_group
        elif isinstance(old_groups, list):
            for old_group in old_groups:
                old_to_new[old_group] = new_group

    old_group_names_specified = list(old_to_new.keys())

    # Check for duplicate old group assignments (would have been overwritten in dict)
    expected_count = sum(1 if isinstance(v, str) else len(v) for v in new_group_mapping.values())
    if len(old_group_names_specified) < expected_count:
        raise ValueError('Cannot remap device groups because a prior group is assigned to more than one new group.')

    # Check for unmapped groups if allow_unspecified is False
    existing_groups = set(results_df[group_var].unique())
    unmapped_groups = existing_groups - set(old_group_names_specified)

    if not allow_unspecified and unmapped_groups:
        raise ValueError(
            f'Cannot remap device groups because allow_unspecified is False and '
            f'the following groups are not remapped: {unmapped_groups}'
        )

    # Apply the renaming
    df = results_df.copy()
    target_col = new_group_column if new_group_column else group_var
    original_values = df[group_var].copy()  # Save before potential overwrite
    df[target_col] = df[group_var].map(old_to_new)

    # For unmapped groups (when allow_unspecified=True), preserve original group name
    if allow_unspecified:
        df[target_col] = df[target_col].fillna(original_values)

    return df


# ==================== Search Refinement Helpers ====================

def exclude_results(main_df, exclude_df, key='MDR_REPORT_KEY'):
    """
    Return rows from main_df whose key values don't appear in exclude_df.

    Useful for search term refinement: find what matches a broad search
    but not a narrow search, to identify potentially missed results.

    Args:
        main_df: DataFrame to filter
        exclude_df: DataFrame containing rows to exclude
        key: Column to match on (default: 'MDR_REPORT_KEY')

    Returns:
        DataFrame with excluded rows removed

    Raises:
        ValueError: If key column is missing from either DataFrame

    Example:
        broad = db.search_by_device_names('omni')
        narrow = db.search_by_device_names([['angiojet', 'omni'], ['boston sci', 'omni']])
        missed = exclude_results(broad, narrow)  # What's in broad but not narrow
    """
    if key not in main_df.columns:
        raise ValueError(f"main_df must contain '{key}' column")
    if key not in exclude_df.columns:
        raise ValueError(f"exclude_df must contain '{key}' column")

    return main_df[~main_df[key].isin(exclude_df[key])]


def filter_by_text(df, exclude_terms=None, include_terms=None, column='DEVICE_NAME_CONCAT'):
    """
    Filter results by text matching on a specified column.

    Useful for removing obvious noise (e.g., insulin pumps) from search results
    or keeping only rows matching certain terms.

    Args:
        df: DataFrame to filter
        exclude_terms: List of terms - exclude rows matching ANY of these
        include_terms: List of terms - keep only rows matching ANY of these
        column: Column to search (default: 'DEVICE_NAME_CONCAT')

    Returns:
        DataFrame with filtered rows

    Raises:
        ValueError: If column is missing from DataFrame

    Example:
        # Remove insulin-related results
        cleaned = filter_by_text(results, exclude_terms=['insulin', 'pump'])

        # Keep only catheter-related results
        catheters = filter_by_text(results, include_terms=['catheter', 'cath'])

        # Combine both
        filtered = filter_by_text(results,
                                  exclude_terms=['insulin'],
                                  include_terms=['thrombectomy'])
    """
    if column not in df.columns:
        raise ValueError(f"DataFrame must contain '{column}' column")

    result = df.copy()

    if exclude_terms:
        pattern = '|'.join(exclude_terms)
        result = result[~result[column].str.contains(pattern, case=False, na=False)]

    if include_terms:
        pattern = '|'.join(include_terms)
        result = result[result[column].str.contains(pattern, case=False, na=False)]

    return result


def summarize_devices(df, columns=None):
    """
    Quick view of unique devices in results for search refinement.

    Shows distinct combinations of device identifiers to help review
    what devices are captured by a search.

    Args:
        df: DataFrame with device information
        columns: List of columns to include. Default: ['BRAND_NAME', 'GENERIC_NAME', 'MANUFACTURER_D_NAME']

    Returns:
        DataFrame with unique device combinations, sorted by first column

    Example:
        missed = exclude_results(broad_search, narrow_search)
        summarize_devices(missed)  # See what devices are being missed
    """
    if columns is None:
        columns = ['BRAND_NAME', 'GENERIC_NAME', 'MANUFACTURER_D_NAME']

    # Only use columns that exist in the DataFrame
    available_cols = [c for c in columns if c in df.columns]

    if not available_cols:
        raise ValueError(f"None of the specified columns found in DataFrame. Available: {list(df.columns)}")

    result = df[available_cols].drop_duplicates()

    if len(available_cols) > 0:
        result = result.sort_values(available_cols[0], na_position='last')

    return result.reset_index(drop=True)


# ==================== Brand Standardization Helpers ====================

def standardize_brand_names(results_df, mapping_dict,
                            source_col='BRAND_NAME',
                            target_col='standard_brand'):
    """
    Standardize brand names using a mapping dictionary.

    Args:
        results_df: DataFrame with brand names
        mapping_dict: Dict mapping patterns to standard names
        source_col: Column with original brand names (default: 'BRAND_NAME')
        target_col: New column for standardized names (default: 'standard_brand')

    Returns:
        DataFrame with new standardized column

    Example:
        mapping = {
            'venovo': 'Venovo',
            'vici': 'Vici',
            'zilver': 'Zilver Vena'
        }
        df = standardize_brand_names(results, mapping)
    """
    if source_col not in results_df.columns:
        raise ValueError(f"DataFrame must contain '{source_col}' column")

    def standardize(brand_name):
        if pd.isna(brand_name):
            return None
        brand_lower = str(brand_name).lower()
        for pattern, standard_name in mapping_dict.items():
            if pattern.lower() in brand_lower:
                return standard_name
        return brand_name  # Keep original if no match

    results_df[target_col] = results_df[source_col].apply(standardize)
    return results_df


def hierarchical_brand_standardization(results_df,
                                       specific_mapping=None,
                                       family_mapping=None,
                                       manufacturer_mapping=None,
                                       source_col='BRAND_NAME',
                                       manufacturer_col='MANUFACTURER_D_NAME'):
    """
    Apply hierarchical brand name standardization with multiple levels.

    This function performs multi-level brand standardization in a single pass:
    1. First tries to match specific device models (e.g., "ClotTriever XL")
    2. Then tries to match device families for unmatched items (e.g., "ClotTriever (unspecified)")
    3. Finally tries to match manufacturers for still-unmatched items (e.g., "Inari Medical")

    Each level only processes items that weren't matched by the previous level,
    preventing "ClotTriever XL" from being incorrectly matched to just "ClotTriever".

    Args:
        results_df: DataFrame with brand names to standardize
        specific_mapping: Dict mapping patterns to specific model names
            Example: {'clottriever xl': 'Inari Medical ClotTriever XL',
                     'clottriever bold': 'Inari Medical ClotTriever BOLD'}
        family_mapping: Dict mapping patterns to device family names
            Example: {'clottriever': 'Inari Medical ClotTriever (unspecified)',
                     'flowtriever': 'Inari Medical FlowTriever (unspecified)'}
        manufacturer_mapping: Dict mapping patterns to manufacturer names
            Example: {'boston': 'Boston Scientific',
                     'inari': 'Inari Medical'}
        source_col: Column with original brand names for model/family matching (default: 'BRAND_NAME')
        manufacturer_col: Column to use for manufacturer matching (default: 'MANUFACTURER_D_NAME')

    Returns:
        DataFrame with three new columns added:
        - device_model: Most specific match (from specific_mapping or family_mapping)
        - device_family: Family-level grouping (from family_mapping if available)
        - manufacturer: Manufacturer name (from manufacturer_mapping if available)

        Original rows are preserved; new columns are None for unmatched items.

    Example:
        >>> specific = {
        ...     'clottriever xl': 'Inari Medical ClotTriever XL',
        ...     'clottriever bold': 'Inari Medical ClotTriever BOLD'
        ... }
        >>> family = {
        ...     'clottriever': 'Inari Medical ClotTriever (unspecified)',
        ...     'flowtriever': 'Inari Medical FlowTriever (unspecified)'
        ... }
        >>> manufacturer = {
        ...     'boston': 'Boston Scientific',
        ...     'inari': 'Inari Medical'
        ... }
        >>> df = hierarchical_brand_standardization(
        ...     results,
        ...     specific_mapping=specific,
        ...     family_mapping=family,
        ...     manufacturer_mapping=manufacturer
        ... )
        >>> # Now df has device_model, device_family, and manufacturer columns
        >>> # "ClotTriever XL" -> device_model="Inari Medical ClotTriever XL"
        >>> # "ClotTriever" -> device_model="Inari Medical ClotTriever (unspecified)"
        >>> # Both -> manufacturer="Inari Medical"

    Notes:
        - Pattern matching is case-insensitive substring matching
        - More specific patterns in each mapping should be listed first (dict order matters)
        - Pass None for any level you don't need
        - Original BRAND_NAME column is preserved unchanged
        - Manufacturer matching uses MANUFACTURER_D_NAME by default (can be overridden)
    """
    if source_col not in results_df.columns:
        raise ValueError(f"DataFrame must contain '{source_col}' column")

    if manufacturer_mapping is not None and manufacturer_col not in results_df.columns:
        raise ValueError(f"DataFrame must contain '{manufacturer_col}' column for manufacturer matching")

    # Create a copy to avoid modifying the original
    df = results_df.copy()

    # Initialize result columns
    df['device_model'] = None
    df['device_family'] = None
    df['manufacturer'] = None

    # Helper function to match a single brand name against a mapping
    def find_match(brand_name, mapping):
        if pd.isna(brand_name) or mapping is None:
            return None
        brand_lower = str(brand_name).lower()
        for pattern, standard_name in mapping.items():
            if pattern.lower() in brand_lower:
                return standard_name
        return None

    # Process each row
    for idx, row in df.iterrows():
        brand_name = row[source_col]

        # Level 1: Try specific mapping
        if specific_mapping:
            specific_match = find_match(brand_name, specific_mapping)
            if specific_match:
                df.at[idx, 'device_model'] = specific_match
                # Also set family if we can derive it from family_mapping
                # Try matching against the brand_name first
                if family_mapping:
                    family_match = find_match(brand_name, family_mapping)
                    if family_match:
                        df.at[idx, 'device_family'] = family_match
                    else:
                        # If brand_name doesn't match family pattern, try matching
                        # against all family patterns to see if the specific model
                        # belongs to any family (e.g., "FlowTriever T16" belongs to "FlowTriever family")
                        for pattern, family_name in family_mapping.items():
                            if pattern.lower() in specific_match.lower():
                                df.at[idx, 'device_family'] = family_name
                                break

        # Level 2: Try family mapping (only if no specific match found)
        if family_mapping and df.at[idx, 'device_model'] is None:
            family_match = find_match(brand_name, family_mapping)
            if family_match:
                # For device_model, append (unspecified) to clarify this is a family-level match
                # But don't add it if already present
                if '(family)' in family_match.lower():
                    # Remove existing (family) suffix and add (family - unspecified)
                    model_name = family_match.replace('(family)', '(family - unspecified)').replace('(Family)', '(family - unspecified)')
                elif '(unspecified)' in family_match.lower():
                    # Already has (unspecified), use as-is
                    model_name = family_match
                else:
                    # Add (unspecified) suffix
                    model_name = f"{family_match} (unspecified)"
                df.at[idx, 'device_model'] = model_name
                df.at[idx, 'device_family'] = family_match

        # Level 3: Try manufacturer mapping (uses manufacturer_col, not brand_name)
        if manufacturer_mapping:
            manufacturer_name = row[manufacturer_col]
            mfr_match = find_match(manufacturer_name, manufacturer_mapping)
            if mfr_match:
                df.at[idx, 'manufacturer'] = mfr_match

    return df


# ==================== Statistical Analysis Methods ====================

def create_contingency_table(results_df, row_var, col_var, normalize=False):
    """
    Create contingency table for chi-square analysis.

    Args:
        results_df: DataFrame with categorical variables
        row_var: Row variable name
        col_var: Column variable name
        normalize: If True, include percentages

    Returns:
        DataFrame (if normalize=False) or dict with 'counts' and 'percentages'

    Example:
        table = create_contingency_table(df, 'standard_brand', 'problem_category', normalize=True)
        print(table['counts'])
        print(table['percentages'])
    """
    if row_var not in results_df.columns:
        raise ValueError(f"DataFrame must contain '{row_var}' column")
    if col_var not in results_df.columns:
        raise ValueError(f"DataFrame must contain '{col_var}' column")

    # Create contingency table
    counts = pd.crosstab(results_df[row_var], results_df[col_var])

    if normalize:
        # Row-wise percentages (sum to 100% per row)
        percentages = counts.div(counts.sum(axis=1), axis=0) * 100
        return {
            'counts': counts,
            'percentages': percentages
        }

    return counts


def chi_square_test(results_df, row_var, col_var, exclude_cols=None):
    """
    Perform chi-square test of independence.

    Args:
        results_df: DataFrame with categorical variables
        row_var: Row variable name
        col_var: Column variable name
        exclude_cols: List of column values to exclude (optional)

    Returns:
        dict with chi2_statistic, p_value, dof, expected_frequencies, significant

    Example:
        result = chi_square_test(df, 'brand', 'problem_category', exclude_cols=['Uncategorized'])
        print(f"Chi-square: {result['chi2_statistic']:.2f}, p={result['p_value']:.4f}")
    """
    from scipy.stats import chi2_contingency

    # Create contingency table
    contingency = create_contingency_table(results_df, row_var, col_var)

    # Exclude specified columns
    if exclude_cols:
        contingency = contingency.drop(columns=exclude_cols, errors='ignore')

    # Perform chi-square test
    chi2, p_value, dof, expected = chi2_contingency(contingency)

    return {
        'chi2_statistic': float(chi2),
        'p_value': float(p_value),
        'dof': int(dof),
        'expected_frequencies': pd.DataFrame(expected,
                                             index=contingency.index,
                                             columns=contingency.columns),
        'significant': p_value < 0.05
    }


def event_type_comparison(results_df, group_var='search_group'):
    """
    Compare event type distributions across groups.

    Args:
        results_df: DataFrame from search_by_device_names() or query_device()
        group_var: Column to compare across (default: 'search_group')

    Returns:
        dict with counts, percentages, chi2_test, summary

    Example:
        # Grouped search
        results = db.search_by_device_names({'g1': 'argon', 'g2': 'penumbra'})
        comparison = event_type_comparison(results)  # Uses search_group
        print(comparison['summary'])

        # Custom grouping
        comparison = event_type_comparison(results, group_var='standard_brand')

    Author: Jacob Schwartz <jaschwa@umich.edu>
    Copyright: 2026, GNU GPL v3
    """
    if 'EVENT_TYPE' not in results_df.columns:
        raise ValueError("DataFrame must contain 'EVENT_TYPE' column")
    if group_var not in results_df.columns:
        raise ValueError(f"DataFrame must contain '{group_var}' column")

    # Deduplicate by MDR_REPORT_KEY
    unique_df = results_df.drop_duplicates(subset=['MDR_REPORT_KEY'], keep='first')

    # Extract event type flags
    unique_df = unique_df.copy()
    event_type_str = unique_df['EVENT_TYPE'].fillna('')
    unique_df['has_death'] = event_type_str.str.contains(r'\bD\b|Death', case=False, regex=True)
    unique_df['has_injury'] = event_type_str.str.contains(r'\bIN\b|Injury', case=False, regex=True)
    unique_df['has_malfunction'] = event_type_str.str.contains(r'\bM\b|Malfunction', case=False, regex=True)

    # Count by group
    counts = unique_df.groupby(group_var).agg({
        'MDR_REPORT_KEY': 'count',
        'has_death': 'sum',
        'has_injury': 'sum',
        'has_malfunction': 'sum'
    }).rename(columns={
        'MDR_REPORT_KEY': 'total',
        'has_death': 'deaths',
        'has_injury': 'injuries',
        'has_malfunction': 'malfunctions'
    })

    # Calculate percentages
    percentages = counts[['deaths', 'injuries', 'malfunctions']].div(counts['total'], axis=0) * 100

    # Chi-square test on event types
    # Create binary columns for each event type
    chi2_df = unique_df[[group_var, 'has_death', 'has_injury', 'has_malfunction']].copy()
    chi2_df_long = pd.melt(chi2_df, id_vars=[group_var],
                           value_vars=['has_death', 'has_injury', 'has_malfunction'],
                           var_name='event_type', value_name='has_event')
    chi2_df_long = chi2_df_long[chi2_df_long['has_event']]  # Only True values
    chi2_result = chi_square_test(chi2_df_long, group_var, 'event_type')

    # Generate summary text
    summary_lines = [
        f"Event Type Comparison by {group_var}",
        "=" * 50,
        f"Chi-square: {chi2_result['chi2_statistic']:.2f} (p={chi2_result['p_value']:.4f})",
        ""
    ]
    for group in percentages.index:
        pcts = percentages.loc[group]
        summary_lines.append(
            f"{group}: {pcts['deaths']:.1f}% deaths, {pcts['injuries']:.1f}% injuries, "
            f"{pcts['malfunctions']:.1f}% malfunctions"
        )

    return {
        'counts': counts,
        'percentages': percentages,
        'chi2_test': chi2_result,
        'summary': '\n'.join(summary_lines)
    }


# ==================== Visualization Methods ====================

def plot_temporal_trends(summary_dict, output_file=None, figsize=(12, 6), **kwargs):
    """
    Generate temporal trend figure.

    Args:
        summary_dict: Output from summarize_by_brand()
        output_file: Path to save figure (optional)
        figsize: Figure size tuple
        **kwargs: Additional matplotlib parameters (title, xlabel, ylabel, etc.)

    Returns:
        Figure and Axes objects

    Example:
        summary = summarize_by_brand(results, include_temporal=True)
        fig, ax = plot_temporal_trends(summary, output_file='figure1.png')
    """
    if 'temporal' not in summary_dict:
        raise ValueError("summary_dict must contain 'temporal' key. "
                        "Set include_temporal=True in summarize_by_brand()")

    temporal_df = summary_dict['temporal']

    fig, ax = plt.subplots(figsize=figsize)

    # Plot each brand as a line
    for brand in temporal_df.index:
        ax.plot(temporal_df.columns, temporal_df.loc[brand],
               marker='o', linewidth=2, markersize=8, label=brand)

    # Styling
    ax.set_xlabel(kwargs.get('xlabel', 'Year'), fontsize=12, fontweight='bold')
    ax.set_ylabel(kwargs.get('ylabel', 'Number of MDRs'), fontsize=12, fontweight='bold')
    ax.set_title(kwargs.get('title', 'Temporal Trends in MAUDE Reports'),
                fontsize=14, fontweight='bold')
    ax.legend(loc=kwargs.get('legend_loc', 'best'), fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_xticks(temporal_df.columns)

    plt.tight_layout()

    if output_file:
        fig.savefig(output_file, dpi=kwargs.get('dpi', 300), bbox_inches='tight')

    return fig, ax


def plot_problem_distribution(contingency_table, output_file=None, stacked=True, **kwargs):
    """
    Generate stacked bar chart for problem distributions.

    Args:
        contingency_table: DataFrame with percentages (from create_contingency_table)
        output_file: Path to save figure (optional)
        stacked: Create stacked (True) or grouped (False) bars
        **kwargs: Additional matplotlib parameters

    Returns:
        Figure and Axes objects

    Example:
        # Grouped search results
        results = db.search_by_device_names({'g1': 'argon', 'g2': 'penumbra'})
        table = create_contingency_table(results, 'search_group', 'category', normalize=True)
        fig, ax = plot_problem_distribution(table['percentages'])

        # Custom xlabel
        fig, ax = plot_problem_distribution(table['percentages'], xlabel='Custom Label')

    Author: Jacob Schwartz <jaschwa@umich.edu>
    Copyright: 2026, GNU GPL v3
    """
    fig, ax = plt.subplots(figsize=kwargs.get('figsize', (12, 6)))

    contingency_table.plot(kind='bar', stacked=stacked, ax=ax,
                          colormap=kwargs.get('colormap', 'Set3'),
                          edgecolor='black', linewidth=0.5)

    # Smart xlabel: use index name if available, otherwise default based on common names
    if 'xlabel' not in kwargs:
        index_name = contingency_table.index.name
        if index_name == 'search_group':
            default_xlabel = 'Search Group'
        elif index_name == 'standard_brand':
            default_xlabel = 'Device Brand'
        elif index_name:
            # Capitalize and replace underscores
            default_xlabel = index_name.replace('_', ' ').title()
        else:
            default_xlabel = 'Group'
        xlabel = default_xlabel
    else:
        xlabel = kwargs['xlabel']

    ax.set_xlabel(xlabel, fontsize=12, fontweight='bold')
    ylabel = 'Percentage (%)' if stacked else 'Count'
    ax.set_ylabel(kwargs.get('ylabel', ylabel), fontsize=12, fontweight='bold')
    ax.set_title(kwargs.get('title', 'Problem Distribution by Device'),
                fontsize=14, fontweight='bold')
    ax.legend(title=kwargs.get('legend_title', 'Category'),
             bbox_to_anchor=(1.05, 1), loc='upper left', fontsize=9)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha='right')

    if stacked:
        ax.set_ylim(0, 100)

    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()

    if output_file:
        fig.savefig(output_file, dpi=kwargs.get('dpi', 300), bbox_inches='tight')

    return fig, ax


def export_publication_figures(db, results_df, output_dir, prefix='figure',
                               formats=['png', 'pdf'], **kwargs):
    """
    Batch export all standard manuscript figures.

    Args:
        db: MaudeDatabase instance (for checking available data)
        results_df: Results DataFrame from search_by_device_names() or query_device()
        output_dir: Output directory path
        prefix: Filename prefix
        formats: List of output formats
        **kwargs: Passed to individual plot functions

    Returns:
        dict mapping figure names to file paths

    Example:
        # Grouped search
        results = db.search_by_device_names({'g1': 'argon', 'g2': 'penumbra'})
        figures = export_publication_figures(db, results, './figures', prefix='study1')

        # With custom brand standardization
        results['standard_brand'] = ...  # Apply standardization
        figures = export_publication_figures(db, results, './figures', group_column='standard_brand')

    Author: Jacob Schwartz <jaschwa@umich.edu>
    Copyright: 2026, GNU GPL v3
    """
    os.makedirs(output_dir, exist_ok=True)
    generated = {}

    # Determine grouping column: prefer search_group, fall back to standard_brand
    if 'search_group' in results_df.columns:
        group_column = 'search_group'
    elif 'standard_brand' in results_df.columns:
        group_column = 'standard_brand'
    else:
        raise ValueError("results_df must have 'search_group' or 'standard_brand' column. "
                        "Use search_by_device_names() with dict input or standardize_brand_names().")

    has_problem_category = 'problem_category' in results_df.columns
    has_patient_category = 'patient_problem_category' in results_df.columns

    # Figure 1: Temporal Trends
    summary = summarize_by_brand(results_df, group_column=group_column, include_temporal=True)
    for fmt in formats:
        fname = f"{output_dir}/{prefix}_temporal_trends.{fmt}"
        plot_temporal_trends(summary, output_file=fname, **kwargs)
        if 'temporal_trends' not in generated:
            generated['temporal_trends'] = []
        generated['temporal_trends'].append(fname)

    # Figure 2: Device Problem Distribution (if available)
    if has_problem_category:
        table = create_contingency_table(results_df, group_column,
                                        'problem_category', normalize=True)
        for fmt in formats:
            fname = f"{output_dir}/{prefix}_device_problems.{fmt}"
            plot_problem_distribution(table['percentages'], output_file=fname,
                                     title='Device Problem Distribution', **kwargs)
            if 'device_problems' not in generated:
                generated['device_problems'] = []
            generated['device_problems'].append(fname)

    # Figure 3: Patient Outcome Distribution (if available)
    if has_patient_category:
        table = create_contingency_table(results_df, group_column,
                                        'patient_problem_category', normalize=True)
        for fmt in formats:
            fname = f"{output_dir}/{prefix}_patient_outcomes.{fmt}"
            plot_problem_distribution(table['percentages'], output_file=fname,
                                     title='Patient Outcome Distribution', **kwargs)
            if 'patient_outcomes' not in generated:
                generated['patient_outcomes'] = []
            generated['patient_outcomes'].append(fname)

    # Figure 4: Event Type Comparison
    comparison = event_type_comparison(results_df, group_var=group_column)
    for fmt in formats:
        fname = f"{output_dir}/{prefix}_event_type_comparison.{fmt}"
        comparison['counts'][['deaths', 'injuries', 'malfunctions']].plot(
            kind='bar', figsize=(10, 6), colormap='viridis'
        )
        plt.xlabel('Device Brand', fontweight='bold')
        plt.ylabel('Number of Events', fontweight='bold')
        plt.title('Event Type Comparison by Device', fontweight='bold')
        plt.legend(['Deaths', 'Injuries', 'Malfunctions'])
        plt.xticks(rotation=45, ha='right')
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(fname, dpi=300, bbox_inches='tight')
        plt.close()
        if 'event_type_comparison' not in generated:
            generated['event_type_comparison'] = []
        generated['event_type_comparison'].append(fname)

    return generated


# ==============================================================================
# EVENT_KEY Deduplication and Validation Functions
# ==============================================================================

def count_unique_events(results_df, event_key_col='EVENT_KEY'):
    """
    Count unique events vs total reports to detect duplication.

    Approximately 8% of MAUDE reports share EVENT_KEY with other reports
    (same event reported by manufacturer, hospital, patient, etc.). This
    function helps identify the duplication rate in your dataset.

    IMPORTANT: Reports with null/missing EVENT_KEY are each considered unique
    events. A missing EVENT_KEY indicates the FDA did not assign a shared event
    identifier, so each report represents a distinct event.

    Args:
        results_df: DataFrame with query results
        event_key_col: Column name for EVENT_KEY (default: 'EVENT_KEY')

    Returns:
        dict with keys:
        - total_reports (int): Total number of reports (rows)
        - unique_events (int): Number of unique events (null EVENT_KEYs each count as 1)
        - duplication_rate (float): Percentage of reports that are duplicates (0-100)
        - multi_report_events (list): EVENT_KEYs that have multiple reports (excludes nulls)

    Example:
        >>> results = db.query_device(device_name='pacemaker')
        >>> stats = count_unique_events(results)
        >>> print(f"Duplication: {stats['duplication_rate']:.1f}%")
        Duplication: 7.8%
    """
    if results_df.empty:
        return {
            'total_reports': 0,
            'unique_events': 0,
            'duplication_rate': 0.0,
            'multi_report_events': []
        }

    if event_key_col not in results_df.columns:
        raise ValueError(f"Column '{event_key_col}' not found in DataFrame. "
                        "EVENT_KEY column is required for event deduplication.")

    total_reports = len(results_df)

    # Count unique non-null EVENT_KEYs
    unique_non_null_events = results_df[event_key_col].nunique()

    # Count null EVENT_KEYs (each null represents a unique event)
    null_count = results_df[event_key_col].isna().sum()

    # Total unique events = non-null unique EVENT_KEYs + each null EVENT_KEY
    unique_events = unique_non_null_events + null_count

    # Find EVENT_KEYs with multiple reports (exclude nulls)
    event_counts = results_df[results_df[event_key_col].notna()].groupby(event_key_col).size()
    multi_report_events = event_counts[event_counts > 1].index.tolist()

    # Calculate duplication rate
    duplicates = total_reports - unique_events
    duplication_rate = (duplicates / total_reports * 100) if total_reports > 0 else 0.0

    return {
        'total_reports': total_reports,
        'unique_events': unique_events,
        'duplication_rate': duplication_rate,
        'multi_report_events': multi_report_events
    }


def detect_multi_report_events(results_df, event_key_col='EVENT_KEY'):
    """
    Identify which events have multiple reports.

    Useful for understanding which specific events have duplicate reporting
    from multiple sources (manufacturer, hospital, patient, etc.).

    Args:
        results_df: DataFrame with query results
        event_key_col: Column name for EVENT_KEY (default: 'EVENT_KEY')

    Returns:
        DataFrame with columns:
        - EVENT_KEY: Event identifier
        - report_count: Number of reports for this event
        - mdr_report_keys: List of MDR_REPORT_KEYs for this event

    Example:
        >>> multi_reports = detect_multi_report_events(results)
        >>> print(f"Found {len(multi_reports)} events with multiple reports")
        >>> print(multi_reports.head())
    """
    if results_df.empty:
        return pd.DataFrame(columns=[event_key_col, 'report_count', 'mdr_report_keys'])

    if event_key_col not in results_df.columns:
        raise ValueError(f"Column '{event_key_col}' not found in DataFrame")

    if 'MDR_REPORT_KEY' not in results_df.columns:
        raise ValueError("Column 'MDR_REPORT_KEY' not found in DataFrame")

    # Group by EVENT_KEY and count reports
    grouped = results_df.groupby(event_key_col).agg(
        report_count=('MDR_REPORT_KEY', 'size'),
        mdr_report_keys=('MDR_REPORT_KEY', lambda x: list(x))
    ).reset_index()

    # Filter to only multi-report events
    multi_reports = grouped[grouped['report_count'] > 1].copy()

    return multi_reports.sort_values('report_count', ascending=False).reset_index(drop=True)


def select_primary_report(results_df, event_key_col='EVENT_KEY',
                          strategy='first_received'):
    """
    When multiple reports exist for same event, select primary report.

    This function manually deduplicates a DataFrame that may contain multiple
    reports for the same event. Note: query_device() now deduplicates by default,
    so this function is mainly useful for analyzing already-retrieved data or
    when working with results from deduplicate_events=False queries.

    Args:
        results_df: DataFrame with query results
        event_key_col: Column name for EVENT_KEY (default: 'EVENT_KEY')
        strategy: Selection strategy (default: 'first_received')
            - 'first_received': Select earliest DATE_RECEIVED
            - 'manufacturer': Prefer manufacturer reports (REPORT_SOURCE_CODE)
            - 'most_complete': Select report with most non-null fields

    Returns:
        DataFrame with one row per unique EVENT_KEY

    Example:
        >>> # Get all reports including duplicates
        >>> all_reports = db.query_device(device_name='stent', deduplicate_events=False)
        >>> # Manually deduplicate by selecting first received
        >>> deduplicated = select_primary_report(all_reports, strategy='first_received')
        >>> print(f"Reduced from {len(all_reports)} to {len(deduplicated)} events")
    """
    if results_df.empty:
        return results_df.copy()

    if event_key_col not in results_df.columns:
        raise ValueError(f"Column '{event_key_col}' not found in DataFrame")

    if strategy == 'first_received':
        if 'DATE_RECEIVED' not in results_df.columns:
            raise ValueError("Strategy 'first_received' requires DATE_RECEIVED column")

        # Convert to datetime if needed
        df = results_df.copy()
        if not pd.api.types.is_datetime64_any_dtype(df['DATE_RECEIVED']):
            df['DATE_RECEIVED'] = pd.to_datetime(df['DATE_RECEIVED'], errors='coerce')

        # Sort by EVENT_KEY and DATE_RECEIVED, then take first
        df_sorted = df.sort_values([event_key_col, 'DATE_RECEIVED'])
        deduplicated = df_sorted.groupby(event_key_col, as_index=False).first()

    elif strategy == 'manufacturer':
        if 'REPORT_SOURCE_CODE' not in results_df.columns:
            # Fallback to first_received if column missing
            return select_primary_report(results_df, event_key_col, 'first_received')

        df = results_df.copy()
        # Prefer manufacturer reports, then take first by any tie-breaker
        df['is_manufacturer'] = df['REPORT_SOURCE_CODE'].str.lower().str.contains('manufacturer', na=False)
        df_sorted = df.sort_values([event_key_col, 'is_manufacturer'], ascending=[True, False])
        deduplicated = df_sorted.groupby(event_key_col, as_index=False).first()
        deduplicated = deduplicated.drop(columns=['is_manufacturer'])

    elif strategy == 'most_complete':
        df = results_df.copy()
        # Count non-null fields per row
        df['_completeness'] = df.notna().sum(axis=1)
        df_sorted = df.sort_values([event_key_col, '_completeness'], ascending=[True, False])
        deduplicated = df_sorted.groupby(event_key_col, as_index=False).first()
        deduplicated = deduplicated.drop(columns=['_completeness'])

    else:
        raise ValueError(f"Unknown strategy: {strategy}. "
                        "Must be 'first_received', 'manufacturer', or 'most_complete'")

    return deduplicated.reset_index(drop=True)


def compare_report_vs_event_counts(results_df, event_key_col='EVENT_KEY',
                                    group_by=None):
    """
    Compare counting by reports vs events to show potential overcounting.

    Demonstrates the impact of EVENT_KEY deduplication on event counts.
    Useful for validating analysis methods and understanding data quality.

    IMPORTANT: Null/missing EVENT_KEYs are each treated as unique events.
    This matches FDA guidance where missing EVENT_KEY indicates each report
    represents a distinct event.

    Args:
        results_df: DataFrame with query results
        event_key_col: Column name for EVENT_KEY (default: 'EVENT_KEY')
        group_by: Optional column to group by (e.g., 'year', 'BRAND_NAME')
                 If None, returns overall counts only

    Returns:
        DataFrame with columns:
        - [group_by column]: If group_by specified
        - report_count: Total number of reports
        - event_count: Number of unique events (null EVENT_KEYs each count as 1)
        - inflation_pct: Percentage inflation from counting reports vs events

    Example:
        >>> # Overall comparison
        >>> comparison = compare_report_vs_event_counts(results)
        >>> print(comparison)

        >>> # By year
        >>> results['year'] = pd.to_datetime(results['DATE_RECEIVED']).dt.year
        >>> yearly = compare_report_vs_event_counts(results, group_by='year')
        >>> print(yearly)
    """
    if results_df.empty:
        cols = ['report_count', 'event_count', 'inflation_pct']
        if group_by:
            cols.insert(0, group_by)
        return pd.DataFrame(columns=cols)

    if event_key_col not in results_df.columns:
        raise ValueError(f"Column '{event_key_col}' not found in DataFrame")

    if group_by:
        if group_by not in results_df.columns:
            raise ValueError(f"Column '{group_by}' not found in DataFrame")

        # Group-wise comparison
        # Need to handle null EVENT_KEYs properly (each null is unique)
        def count_unique_with_nulls(series):
            """Count unique values treating each null as unique."""
            non_null_unique = series.nunique()
            null_count = series.isna().sum()
            return non_null_unique + null_count

        grouped = results_df.groupby(group_by).agg({
            'MDR_REPORT_KEY': 'count',  # Total reports
            event_key_col: count_unique_with_nulls  # Unique events (nulls are unique)
        }).reset_index()

        grouped.columns = [group_by, 'report_count', 'event_count']

    else:
        # Overall comparison
        # Count null EVENT_KEYs as unique events
        non_null_unique = results_df[event_key_col].nunique()
        null_count = results_df[event_key_col].isna().sum()
        unique_events = non_null_unique + null_count

        grouped = pd.DataFrame({
            'report_count': [len(results_df)],
            'event_count': [unique_events]
        })

    # Calculate inflation percentage
    grouped['inflation_pct'] = ((grouped['report_count'] - grouped['event_count']) /
                                grouped['event_count'] * 100).fillna(0.0)

    return grouped


# ==============================================================================
# Patient OUTCOME Concatenation Detection Functions
# ==============================================================================

def detect_multi_patient_reports(patient_df):
    """
    Detect reports with multiple patients (potential outcome concatenation).

    In MAUDE data, when multiple patients are involved in a single report,
    the OUTCOME fields concatenate sequentially across patients, leading to
    inflated outcome counts. This function identifies affected reports.

    Args:
        patient_df: DataFrame with patient data (must have MDR_REPORT_KEY column)

    Returns:
        dict with keys:
        - total_reports (int): Total unique MDR_REPORT_KEYs
        - multi_patient_reports (int): Count of reports with >1 patient
        - affected_percentage (float): % of reports affected (0-100)
        - affected_mdr_keys (list): MDR_REPORT_KEYs with multiple patients

    Example:
        >>> patient_data = db.enrich_with_patient_data(results)
        >>> validation = detect_multi_patient_reports(patient_data)
        >>> if validation['affected_percentage'] > 10:
        ...     print(f"Warning: {validation['affected_percentage']:.1f}% have multiple patients")
    """
    if patient_df.empty:
        return {
            'total_reports': 0,
            'multi_patient_reports': 0,
            'affected_percentage': 0.0,
            'affected_mdr_keys': []
        }

    if 'MDR_REPORT_KEY' not in patient_df.columns:
        raise ValueError("Column 'MDR_REPORT_KEY' not found in DataFrame")

    # Count patients per report
    patients_per_report = patient_df.groupby('MDR_REPORT_KEY').size()

    total_reports = len(patients_per_report)
    multi_patient_reports = (patients_per_report > 1).sum()
    affected_percentage = (multi_patient_reports / total_reports * 100) if total_reports > 0 else 0.0

    # Get list of affected MDR_REPORT_KEYs
    affected_mdr_keys = patients_per_report[patients_per_report > 1].index.tolist()

    return {
        'total_reports': total_reports,
        'multi_patient_reports': multi_patient_reports,
        'affected_percentage': affected_percentage,
        'affected_mdr_keys': affected_mdr_keys
    }


def count_unique_outcomes_per_report(patient_df, outcome_col='SEQUENCE_NUMBER_OUTCOME'):
    """
    Count unique outcome codes per report, preventing inflation from concatenation.

    When multiple patients share a report, outcome fields concatenate sequentially.
    This function counts each outcome code ONCE per report, regardless of how many
    times it appears in concatenated fields.

    Args:
        patient_df: DataFrame with patient data
        outcome_col: Column name for outcomes (default: 'SEQUENCE_NUMBER_OUTCOME')

    Returns:
        DataFrame with columns:
        - MDR_REPORT_KEY: Report identifier
        - patient_count: Number of patients in this report
        - unique_outcomes: List of unique outcome codes for this report
        - outcome_counts: Dict with count of each outcome code
        - Plus any columns that are consistent within each MDR_REPORT_KEY group
          (e.g., search_group, BRAND_NAME, EVENT_TYPE)

    Example:
        >>> patient_data = db.enrich_with_patient_data(results)
        >>> outcome_summary = count_unique_outcomes_per_report(patient_data)
        >>> # Count reports with at least one death (avoiding concatenation inflation)
        >>> death_count = (outcome_summary['unique_outcomes'].apply(lambda x: 'D' in x)).sum()
        >>> print(f"Reports with deaths: {death_count}")
    """
    if patient_df.empty:
        return pd.DataFrame(columns=['MDR_REPORT_KEY', 'patient_count', 'unique_outcomes', 'outcome_counts'])

    if 'MDR_REPORT_KEY' not in patient_df.columns:
        raise ValueError("Column 'MDR_REPORT_KEY' not found in DataFrame")

    if outcome_col not in patient_df.columns:
        raise ValueError(f"Column '{outcome_col}' not found in DataFrame")

    def extract_outcomes(outcome_str):
        """Extract individual outcome codes from semicolon-separated string."""
        if pd.isna(outcome_str):
            return set()
        return {code.strip() for code in str(outcome_str).split(';') if code.strip()}

    # Identify columns to preserve (same value for all rows with same MDR_REPORT_KEY)
    # Skip patient-specific columns and the outcome column
    skip_prefixes = ('PATIENT', 'SEQUENCE', 'outcome')
    preserve_cols = []
    for col in patient_df.columns:
        if col in ['MDR_REPORT_KEY', outcome_col, 'outcome_codes']:
            continue
        if any(col.upper().startswith(prefix.upper()) for prefix in skip_prefixes):
            continue
        # Check if column has single value per key (sample check for performance)
        sample_keys = patient_df['MDR_REPORT_KEY'].unique()[:100]
        sample_df = patient_df[patient_df['MDR_REPORT_KEY'].isin(sample_keys)]
        if len(sample_df) > 0 and sample_df.groupby('MDR_REPORT_KEY')[col].nunique().max() == 1:
            preserve_cols.append(col)

    # For each report, collect all outcomes from all patients and deduplicate
    report_outcomes = []

    for mdr_key, group in patient_df.groupby('MDR_REPORT_KEY'):
        all_outcomes = set()
        for outcome_str in group[outcome_col]:
            all_outcomes.update(extract_outcomes(outcome_str))

        outcome_list = sorted(all_outcomes)
        outcome_counts_dict = {code: 1 for code in outcome_list}  # Each outcome counted once per report

        row_data = {
            'MDR_REPORT_KEY': mdr_key,
            'patient_count': len(group),
            'unique_outcomes': outcome_list,
            'outcome_counts': outcome_counts_dict
        }

        # Preserve additional columns
        for col in preserve_cols:
            row_data[col] = group[col].iloc[0]

        report_outcomes.append(row_data)

    return pd.DataFrame(report_outcomes)
