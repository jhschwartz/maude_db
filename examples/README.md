# MAUDE Database Examples

Example scripts demonstrating how to use the `maude_db` library to analyze FDA MAUDE adverse event data.

## Setup

```bash
# From the maude_db directory
cd scripts/maude_db
source venv/bin/activate

# Install matplotlib for visualizations (if not already installed)
pip install matplotlib
```

## Examples

### 1. Basic Usage (`basic_usage.py`)

Minimal example showing core functionality.

```bash
cd examples
python basic_usage.py
```

**What it does:**
- Downloads 1998 device data (small file, ~3MB)
- Shows database summary
- Runs simple SQL queries
- Searches for specific device types

### 2. Device Trend Analysis (`analyze_device_trends.py`)

Comprehensive example analyzing adverse event trends over time.

```bash
cd examples
python analyze_device_trends.py
```

**What it does:**
- Downloads 3 years of device + narrative data (2018-2020)
- Queries for specific devices (thrombectomy by default)
- Analyzes trends: deaths, injuries, malfunctions over time
- Retrieves and displays event narratives
- Exports results to CSV
- Creates trend visualizations

**Outputs:**
- `thrombectomy_events.csv` - All matching events
- `thrombectomy_trends.png` - Trend charts
- `maude_trends.db` - SQLite database with downloaded data

## Customization

### Analyze Different Devices

Edit `analyze_device_trends.py`:

```python
DEVICE_NAME = 'pacemaker'  # or 'stent', 'catheter', etc.
YEARS = '2020-2022'        # adjust year range
```

### Query Specific Product Codes

```python
# Instead of device name, use product code
device_events = db.query_device(product_code='NIQ')
```

### Custom SQL Queries

```python
# Raw SQL for complex analysis
result = db.query("""
    SELECT
        d.GENERIC_NAME,
        COUNT(*) as events,
        SUM(CASE WHEN m.event_type LIKE '%Death%' THEN 1 ELSE 0 END) as deaths
    FROM device d
    JOIN master m ON d.MDR_REPORT_KEY = m.mdr_report_key
    WHERE d.GENERIC_NAME LIKE '%your_device%'
    GROUP BY d.GENERIC_NAME
    ORDER BY deaths DESC
""")
```

## Data Files

Downloaded data is cached in `./maude_data/` within the examples directory. Files are reused on subsequent runs, so re-running scripts is fast.

## Notes

- First run downloads data from FDA servers (may take a few minutes)
- Subsequent runs use cached files
- SQLite database grows with more years of data
- For large datasets (10+ years), consider using specific date ranges