#!/usr/bin/env python3
"""
Example: Analyzing Medical Device Adverse Event Trends

This script demonstrates how to use the MaudeDatabase class to:
1. Download FDA MAUDE data for specific years
2. Query device-specific adverse events
3. Analyze trends over time
4. Export results to CSV

Usage:
    python analyze_device_trends.py
"""

import sys
import os

# Add parent directory to path to import maude_db
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from maude_db import MaudeDatabase
import matplotlib.pyplot as plt
import pandas as pd


def main():
    # Configuration
    DB_PATH = 'maude_trends.db'
    DATA_DIR = './maude_data'
    DEVICE_NAME = 'thrombectomy'  # Search for thrombectomy devices
    YEARS = '2018-2020'  # Analyze 3 years of data

    print("="*60)
    print("MAUDE Device Adverse Event Analysis Example")
    print("="*60)

    # Step 1: Initialize database
    print(f"\n1. Connecting to database: {DB_PATH}")
    db = MaudeDatabase(DB_PATH, verbose=True)

    # Step 2: Download data (if not already present)
    print(f"\n2. Downloading MAUDE data for years {YEARS}...")
    print("   Note: This downloads device and text (narrative) data")
    print("   Files are cached, so re-running is fast.\n")

    db.add_years(
        years=YEARS,
        tables=['device', 'text'],  # Get device info and narratives
        download=True,
        data_dir=DATA_DIR
    )

    # Step 3: Show database info
    print("\n3. Database contents:")
    db.info()

    # Step 4: Query for specific device
    print(f"\n4. Querying for '{DEVICE_NAME}' devices...")
    device_events = db.query_device(device_name=DEVICE_NAME)

    print(f"   Found {len(device_events):,} adverse events")

    if len(device_events) > 0:
        print(f"\n   Sample event (first record):")
        print(f"   - Report Key: {device_events.iloc[0]['MDR_REPORT_KEY']}")
        print(f"   - Device: {device_events.iloc[0]['GENERIC_NAME']}")
        print(f"   - Brand: {device_events.iloc[0]['BRAND_NAME']}")
        print(f"   - Date: {device_events.iloc[0]['DATE_RECEIVED']}")

    # Step 5: Analyze trends over time
    print(f"\n5. Analyzing trends for '{DEVICE_NAME}' devices...")
    trends = db.get_trends_by_year(device_name=DEVICE_NAME)

    print("\n   Yearly breakdown:")
    print(trends.to_string(index=False))

    # Step 6: Get sample narratives
    print(f"\n6. Retrieving sample event narratives...")
    if len(device_events) > 0:
        # Get narratives for first 3 events
        sample_keys = device_events['MDR_REPORT_KEY'].head(3).tolist()
        narratives = db.get_narratives(sample_keys)

        print(f"   Retrieved {len(narratives)} narratives\n")
        for i, row in narratives.iterrows():
            text = row['FOI_TEXT']
            # Truncate long narratives
            if len(text) > 200:
                text = text[:200] + "..."
            print(f"   Event {row['MDR_REPORT_KEY']}:")
            print(f"   {text}\n")

    # Step 7: Export to CSV
    output_file = f'{DEVICE_NAME}_events.csv'
    print(f"7. Exporting results to {output_file}...")
    db.export_subset(output_file, device_name=DEVICE_NAME)

    # Step 8: Create visualization
    print(f"\n8. Creating trend visualization...")
    if len(trends) > 0:
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

        # Plot 1: Total events over time
        ax1.plot(trends['year'], trends['event_count'], marker='o', linewidth=2)
        ax1.set_xlabel('Year')
        ax1.set_ylabel('Number of Events')
        ax1.set_title(f'{DEVICE_NAME.title()} Device\nAdverse Events Over Time')
        ax1.grid(True, alpha=0.3)

        # Plot 2: Event types breakdown
        event_types = ['deaths', 'injuries', 'malfunctions']
        colors = ['#d62728', '#ff7f0e', '#1f77b4']

        for event_type, color in zip(event_types, colors):
            ax2.plot(trends['year'], trends[event_type],
                    marker='o', label=event_type.title(),
                    linewidth=2, color=color)

        ax2.set_xlabel('Year')
        ax2.set_ylabel('Number of Events')
        ax2.set_title(f'{DEVICE_NAME.title()} Device\nEvent Types Over Time')
        ax2.legend()
        ax2.grid(True, alpha=0.3)

        plt.tight_layout()

        plot_file = f'{DEVICE_NAME}_trends.png'
        plt.savefig(plot_file, dpi=150, bbox_inches='tight')
        print(f"   Saved visualization to {plot_file}")

        # Optionally show the plot
        # plt.show()

    # Clean up
    db.close()

    print("\n" + "="*60)
    print("Analysis complete!")
    print("="*60)
    print(f"\nGenerated files:")
    print(f"  - Database: {DB_PATH}")
    print(f"  - CSV export: {output_file}")
    if len(trends) > 0:
        print(f"  - Visualization: {plot_file}")
    print()


if __name__ == '__main__':
    main()