# Research Guide

Practical guide for using MAUDE data in medical device research.

## Table of Contents

1. [Planning Your Analysis](#planning-your-analysis)
2. [Which Data to Download](#which-data-to-download)
3. [Common Analysis Patterns](#common-analysis-patterns)
4. [Data Quality Tips](#data-quality-tips)
5. [Working with Large Datasets](#working-with-large-datasets)
6. [Reproducibility](#reproducibility)
7. [Common Research Workflows](#common-research-workflows)
8. [Understanding Limitations](#understanding-limitations)
9. [Citing Your Work](#citing-your-work)

---

## Planning Your Analysis

### Define Your Research Question

Start with a clear, specific question:

**Good examples**:
- "How have thrombectomy device adverse events changed from 2015-2020?"
- "What are the most common failure modes for drug-eluting stents?"
- "How does event reporting differ between manufacturer vs. user facility reports?"

**Too vague**:
- "Are pacemakers safe?" (needs denominator data, causation)
- "What devices have problems?" (too broad)

### Determine Your Time Range

Consider:
- **Reporting changes**: FDA requirements have evolved
- **Device evolution**: Newer models may have different profiles
- **Sample size**: More years = more events but slower downloads
- **Recent data**: Latest year may be incomplete

**Quick start**: 3-5 recent years gives good trends without overwhelming data.

```python
# Good starting point for trend analysis
db.add_years('2018-2022', tables=['device'], download=True)
```

### Choose Your Device Scope

Options for identifying devices:

1. **Generic name**: Broad category (`'catheter'`, `'pacemaker'`)
2. **Product code**: Specific device type (`'NIQ'`)
3. **Brand name**: Specific manufacturer product (`'Trevo'`)

**Tip**: Start broad, then narrow:

```python
# Step 1: See what's available
all_catheters = db.query_device(device_name='catheter')
print(all_catheters['GENERIC_NAME'].value_counts())

# Step 2: Pick specific type
specific = db.query_device(device_name='intravascular catheter')
```

---

## Which Data to Download

### Essential Tables

**For most research**:
```python
db.add_years('2018-2020', tables=['device'], download=True)
```

- **device**: Device information (brand, generic name, manufacturer)
- Good for: Counting events, trends, device comparisons

**Add for richer analysis**:
```python
db.add_years('2018-2020', tables=['device', 'text'], download=True)
```

- **text**: Narrative descriptions of events
- Good for: Qualitative analysis, understanding failure modes

**Add for patient demographics**:
```python
db.add_years('2018-2020', tables=['device', 'patient'], download=True)
```

- **patient**: Age, outcomes, event dates
- Good for: Patient outcome analysis

### Disk Space Considerations

Approximate sizes per year:

| Tables | Size/Year | 5 Years Total |
|--------|-----------|---------------|
| device only | ~45 MB | ~225 MB |
| device + text | ~90 MB | ~450 MB |
| device + text + patient | ~100 MB | ~500 MB |

**Tip**: Start with device table, add others if needed.

### Master Table Note

The master table (MDRFOI) is only available as a comprehensive file covering all years:
- Very large (~1+ GB)
- Not downloaded automatically by `maude_db`
- Device table contains most key fields via joins

For most research, device + text tables are sufficient.

---

## Common Analysis Patterns

### Pattern 1: Count Events Over Time

```python
# Simple trend
trends = db.get_trends_by_year(device_name='pacemaker')
print(trends)

# Visualize
import matplotlib.pyplot as plt
plt.plot(trends['year'], trends['event_count'])
plt.xlabel('Year')
plt.ylabel('Events Reported')
plt.title('Pacemaker Adverse Events')
plt.show()
```

### Pattern 2: Compare Device Types

```python
# Get events for multiple device types
catheters = db.query_device(device_name='catheter')
stents = db.query_device(device_name='stent')

print(f"Catheter events: {len(catheters)}")
print(f"Stent events: {len(stents)}")
```

### Pattern 3: Identify Top Generic Names

```python
devices = db.query_device(start_date='2020-01-01', end_date='2020-12-31')

# Count by generic name
top_devices = devices['GENERIC_NAME'].value_counts().head(10)
print(top_devices)
```

### Pattern 4: Analyze Event Types

```python
results = db.query_device(device_name='defibrillator')

# Count event types
print(results['event_type'].value_counts())

# Filter to serious events only
serious = results[results['event_type'].str.contains('Death|Injury', na=False)]
print(f"Serious events: {len(serious)}/{len(results)}")
```

### Pattern 5: Examine Narratives

```python
# Get events
events = db.query_device(device_name='thrombectomy', start_date='2020-01-01')

# Sample some events
sample_keys = events['MDR_REPORT_KEY'].sample(10).tolist()

# Get narratives
narratives = db.get_narratives(sample_keys)

# Look for patterns
for idx, row in narratives.iterrows():
    text = row['FOI_TEXT'].lower()
    if 'fracture' in text or 'break' in text:
        print(f"Report {row['MDR_REPORT_KEY']}: {row['FOI_TEXT'][:150]}")
```

### Pattern 6: Filter by Manufacturer

```python
results = db.query_device(device_name='stent')

# Count by manufacturer
mfr_counts = results['manufacturer_d_name'].value_counts()
print(mfr_counts.head())

# Filter to specific manufacturer
medtronic = results[results['manufacturer_d_name'].str.contains('Medtronic', na=False, case=False)]
```

---

## Data Quality Tips

### Check for Completeness

```python
# How many events have narratives?
devices = db.query_device(device_name='catheter')
keys = devices['MDR_REPORT_KEY'].tolist()
narratives = db.get_narratives(keys)

print(f"Events: {len(devices)}")
print(f"With narratives: {len(narratives)}")
print(f"Coverage: {len(narratives)/len(devices)*100:.1f}%")
```

### Handle Missing Data

```python
results = db.query_device(device_name='pacemaker')

# Check for missing brand names
missing_brand = results['BRAND_NAME'].isna().sum()
print(f"Missing brand names: {missing_brand}/{len(results)}")

# Filter to complete records
complete = results.dropna(subset=['BRAND_NAME', 'GENERIC_NAME'])
```

### Understand Reporting Bias

Be aware that:
- **Voluntary reporting** is inconsistent
- **High-profile events** increase reporting temporarily
- **New devices** may have higher reporting (heightened awareness)
- **Old devices** may be underreported (less attention)

**Tip**: Use year-over-year trends to identify real patterns vs. reporting artifacts.

### Verify Unexpected Results

If you see surprising patterns:

```python
# Sudden spike in events?
trends = db.get_trends_by_year(device_name='insulin pump')
# Look at year-to-year changes

# Unusual event count?
devices = db.query_device(device_name='unusual_device')
# Check sample narratives to verify it's actually your target device
```

---

## Working with Large Datasets

### Efficient Querying

**Do**: Filter early
```python
# Good - filter in query
recent_pacers = db.query_device(
    device_name='pacemaker',
    start_date='2020-01-01'
)
```

**Don't**: Load everything then filter
```python
# Inefficient
all_devices = db.query("SELECT * FROM device")  # Loads millions of rows
pacers = all_devices[all_devices['GENERIC_NAME'].str.contains('pacemaker')]
```

### Chunked Processing

For very large result sets:

```python
# Process in chunks
query = """
    SELECT MDR_REPORT_KEY, GENERIC_NAME
    FROM device
    WHERE GENERIC_NAME LIKE '%catheter%'
"""

for chunk in pd.read_sql_query(query, db.conn, chunksize=10000):
    # Process each chunk
    print(f"Processing {len(chunk)} records...")
    # Your analysis here
```

### Memory Management

```python
# Free memory after large operations
import gc

large_result = db.query_device(device_name='catheter')
# ... use large_result ...
del large_result
gc.collect()
```

---

## Reproducibility

### Document Your Setup

Create a README for your project:

```markdown
# My Analysis

## Environment
- Python 3.9.7
- maude_db version: [commit hash or release]
- Analysis date: 2024-12-29

## Data
- Years: 2018-2020
- Tables: device, text
- Downloaded: 2024-12-29
- FDA MAUDE access: https://www.fda.gov/...
```

### Save Your Queries

Keep analysis scripts:

```python
# analysis.py
from maude_db import MaudeDatabase

# Setup
db = MaudeDatabase('maude_2018_2020.db')

# Analysis 1: Trend analysis
trends = db.get_trends_by_year(device_name='thrombectomy')
trends.to_csv('results/thrombectomy_trends.csv', index=False)

# Analysis 2: Event type breakdown
events = db.query_device(device_name='thrombectomy')
breakdown = events['event_type'].value_counts()
breakdown.to_csv('results/event_type_breakdown.csv')
```

### Version Your Database

```bash
# Create dated database snapshots
cp maude.db backups/maude_2024-12-29.db
```

### Track Data Versions

```python
# Save metadata with results
import datetime

metadata = {
    'analysis_date': datetime.date.today().isoformat(),
    'database': 'maude.db',
    'years': '2018-2020',
    'device': 'thrombectomy',
    'event_count': len(results)
}

import json
with open('results/metadata.json', 'w') as f:
    json.dump(metadata, f, indent=2)
```

---

## Common Research Workflows

### Workflow 1: Device Safety Surveillance

**Goal**: Monitor adverse events for a specific device type

```python
db = MaudeDatabase('surveillance.db')

# Download recent years
db.add_years('2020-2023', tables=['device', 'text'], download=True)

# Query device
events = db.query_device(device_name='insulin pump')

# Analyze trends
trends = db.get_trends_by_year(device_name='insulin pump')

# Identify serious events
serious = events[events['event_type'].str.contains('Death|Injury', na=False)]

# Examine narratives of serious events
serious_keys = serious['MDR_REPORT_KEY'].head(20).tolist()
narratives = db.get_narratives(serious_keys)

# Export for detailed review
serious.to_csv('serious_events.csv', index=False)
```

### Workflow 2: Comparative Device Study

**Goal**: Compare safety profiles of two device types

```python
db = MaudeDatabase('comparison.db')
db.add_years('2018-2022', tables=['device'], download=True)

# Get events for each device
device_a = db.query_device(product_code='ABC')
device_b = db.query_device(product_code='XYZ')

# Compare event types
print("Device A event types:")
print(device_a['event_type'].value_counts())

print("\nDevice B event types:")
print(device_b['event_type'].value_counts())

# Trend comparison
trends_a = db.get_trends_by_year(product_code='ABC')
trends_b = db.get_trends_by_year(product_code='XYZ')

# Visualize
import matplotlib.pyplot as plt
plt.plot(trends_a['year'], trends_a['event_count'], label='Device A')
plt.plot(trends_b['year'], trends_b['event_count'], label='Device B')
plt.legend()
plt.show()
```

### Workflow 3: Failure Mode Analysis

**Goal**: Identify common device failure patterns

```python
db = MaudeDatabase('failure_modes.db')
db.add_years('2020-2022', tables=['device', 'text'], download=True)

# Get device events
events = db.query_device(device_name='cardiac catheter')

# Get all narratives
keys = events['MDR_REPORT_KEY'].tolist()
narratives = db.get_narratives(keys)

# Search narratives for failure keywords
keywords = ['fracture', 'break', 'disconnect', 'leak', 'malfunction']

for keyword in keywords:
    count = narratives['FOI_TEXT'].str.contains(keyword, case=False, na=False).sum()
    print(f"{keyword}: {count} mentions")

# Extract examples
fractures = narratives[narratives['FOI_TEXT'].str.contains('fracture', case=False, na=False)]
print(f"\n{len(fractures)} fracture-related events")
```

### Workflow 4: Temporal Analysis

**Goal**: Identify when event reporting changed

```python
db = MaudeDatabase('temporal.db')
db.add_years('2015-2023', tables=['device'], download=True)

# Get long-term trends
trends = db.get_trends_by_year(device_name='pacemaker')

# Calculate year-over-year changes
trends['yoy_change'] = trends['event_count'].pct_change() * 100

print("Year-over-year change in reporting:")
print(trends[['year', 'event_count', 'yoy_change']])

# Identify notable changes
notable = trends[abs(trends['yoy_change']) > 20]
print(f"\nYears with >20% change: {notable['year'].tolist()}")
```

---

## Understanding Limitations

### What MAUDE Can Tell You

- **Adverse events occurred**: Reports document what happened
- **Patterns exist**: Trends and common issues
- **Signals for investigation**: Potential safety concerns

### What MAUDE Cannot Tell You

- **Causation**: Reports don't prove device caused event
- **Incidence rates**: No denominator (devices in use)
- **Comparison across devices**: Different usage rates, reporting patterns
- **Complete picture**: Voluntary reporting means many events unreported

### Using MAUDE Responsibly

**Do**:
- Use for signal detection and hypothesis generation
- Combine with other data sources (literature, registries)
- Consider reporting biases in interpretation
- Note limitations in publications

**Don't**:
- Claim causation from MAUDE data alone
- Compare raw counts across dissimilar devices
- Assume absence of reports means device is safe
- Use single reports as definitive evidence

### Example Methods Section

For publications using MAUDE data:

> We analyzed FDA MAUDE adverse event reports for [device type] from [years].
> MAUDE contains voluntary and mandatory reports of device-related deaths,
> injuries, and malfunctions. Reports are unverified allegations and do not
> establish causation. We used MAUDE data for signal detection and hypothesis
> generation, recognizing inherent reporting biases and the lack of denominator
> data on total devices in use. Analysis was performed using the maude_db
> Python library (version X.X, Schwartz 2024).

---

## Citing Your Work

### Citing MAUDE Data

Always cite the FDA database:

```
U.S. Food and Drug Administration. (2024). MAUDE - Manufacturer and User
Facility Device Experience Database. Retrieved [date] from
https://www.fda.gov/medical-devices/mandatory-reporting-requirements-manufacturers-importers-and-device-user-facilities/manufacturer-and-user-facility-device-experience-database-maude
```

### Citing This Library

```
Schwartz, J. (2024). maude_db: A Python library for FDA MAUDE database analysis.
University of Michigan Medical School. https://github.com/[your-repo]
```

### In-Text Example

> We queried the FDA MAUDE database (FDA 2024) using the maude_db Python
> library (Schwartz 2024) for all adverse event reports involving thrombectomy
> devices from 2018-2022.

### Data Availability Statement

> FDA MAUDE data is publicly available at https://www.fda.gov/medical-devices/
> [...]. Our analysis code and processed datasets are available at [your repo].

---

## Next Steps

- **API Reference**: See [api_reference.md](api_reference.md) for complete method documentation
- **Examples**: Check [`examples/`](../examples/) for working code
- **Troubleshooting**: See [troubleshooting.md](troubleshooting.md) for common issues

**Questions?** Open an issue on GitHub or consult the FDA MAUDE documentation.