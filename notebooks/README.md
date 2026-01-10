# PyMAUDE Jupyter Notebooks

Interactive examples for the PyMAUDE library. These notebooks demonstrate real research workflows for analyzing FDA MAUDE adverse event data.

## Notebooks

Run notebooks locally to use recent data and demonstrate complete helper methods.

### Setup

```bash
# From PyMAUDE directory
cd PyMAUDE
source venv/bin/activate  # Or: venv\Scripts\activate on Windows

# Install visualization dependencies
pip install matplotlib seaborn

# Launch Jupyter
jupyter notebook notebooks/
```

## Notebook Guide

### Beginner Track 🟢

Start here if you're new to PyMAUDE:

1. **[01_getting_started.ipynb](01_getting_started.ipynb)**
   - **Data**: 2022-2023, device + master (~300MB)
   - **Runtime**: ~4 minutes
   - **Learn**: Helper methods, event type breakdowns, multi-year downloads
   - **Best for**: Understanding the full API

2. **[02_trend_analysis_visualization.ipynb](02_trend_analysis_visualization.ipynb)**
   - **Data**: 2020-2023, device + master (~800MB)
   - **Runtime**: ~8 minutes
   - **Learn**: Temporal analysis, matplotlib visualizations, publication figures
   - **Best for**: Your first real analysis

### Intermediate Track 🟡

Dive deeper into specific analysis patterns:

3. **[03_advanced_querying.ipynb](03_advanced_querying.ipynb)**
   - **Data**: 2022-2023
   - **Runtime**: ~6 minutes
   - **Learn**: Complex SQL, multi-table joins, custom aggregations
   - **Best for**: Users comfortable with SQL

4. **[04_manufacturer_comparison.ipynb](04_manufacturer_comparison.ipynb)**
   - **Data**: 2020-2023
   - **Runtime**: ~8 minutes
   - **Learn**: Comparative analysis, statistical considerations
   - **Best for**: Safety surveillance, competitive intelligence

7. **[07_helper_methods_reference.ipynb](07_helper_methods_reference.ipynb)**
   - **Data**: 1998 + 2022 (demonstrations)
   - **Runtime**: ~5 minutes
   - **Learn**: Complete API reference with examples
   - **Best for**: Quick reference while coding

### Advanced Track 🔴

For experienced researchers:

5. **[05_signal_detection.ipynb](05_signal_detection.ipynb)**
   - **Data**: 2020-2023
   - **Runtime**: ~10 minutes
   - **Learn**: Temporal spike detection, proportion analysis, statistical thresholds
   - **Best for**: Safety surveillance teams

6. **[06_reproducible_research_workflow.ipynb](06_reproducible_research_workflow.ipynb)**
   - **Data**: 2020-2023
   - **Runtime**: ~5 minutes
   - **Learn**: Publication best practices, data provenance, archiving
   - **Best for**: Preparing manuscripts, sharing with collaborators

## Learning Paths

### I want to...

**...try PyMAUDE quickly (5 min):**
- Start: 01 → Done!

**...analyze a specific device (30 min):**
- Path: 01 → 02

**...prepare a publication (2 hours):**
- Path: 01 → 02 → 04 → 06

**...do safety surveillance (advanced):**
- Path: 01 → 02 → 04 → 05

**...learn the complete API:**
- Path: 01 → 07 → 03

## Data Download Notes

### First Run Times

Notebooks download data from FDA on first run. Subsequent runs use cached data:

- **Notebook 01**: ~4 min (300MB)
- **Notebook 02-07**: ~8-10 min first run (800MB-1.5GB depending on years)

### Data is Cached

Downloaded data is stored in `./maude_data/` and reused across notebooks. After your first multi-year download, other notebooks run much faster.

### Disk Space Requirements

- Notebooks 01-02: ~500MB
- All notebooks 01-07: ~2GB

## Troubleshooting

### Import Errors

If you see `ModuleNotFoundError: No module named 'PyMAUDE'`:

```python
# Run this in the first cell:
import sys
from pathlib import Path
sys.path.insert(0, str(Path().resolve().parent / 'src'))
```

This is already included in all notebooks but may need adjustment if you moved files.

### Matplotlib Not Found

For notebooks with visualizations (03, 05, 06):

```bash
pip install matplotlib seaborn
```

### Slow Downloads

If FDA servers are slow:
- Try a different time of day
- Data is cached after first download
- Consider notebooks with less data (01-02)

### Database Locked

If you see "database is locked":
- Close any open database connections
- Only run one notebook at a time
- Restart Jupyter kernel

## Contributing

Found a bug or have suggestions for new notebooks? Please open an issue or PR at the main repository.

## Additional Resources

- **[../docs/getting_started.md](../docs/getting_started.md)** - Detailed installation guide
- **[../docs/api_reference.md](../docs/api_reference.md)** - Complete API documentation
- **[../docs/maude_overview.md](../docs/maude_overview.md)** - Understanding MAUDE database structure
- **[../docs/research_guide.md](../docs/research_guide.md)** - Best practices for device research
- **[../README.md](../README.md)** - Main project documentation

## Questions?

- Review the [troubleshooting guide](../docs/troubleshooting.md)
- Check [existing issues](https://github.com/yourusername/PyMAUDE/issues)
- Open a new issue for bugs or feature requests
