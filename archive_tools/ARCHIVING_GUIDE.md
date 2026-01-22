# MAUDE Database Archiving Guide

Quick reference for archiving and monitoring your MAUDE database.

## Overview

Two new scripts help protect against FDA website changes:

1. **prepare_zenodo_archive.py** - Create complete database archives for Zenodo
2. **check_fda_compatibility.py** - Monitor FDA website compatibility

## Quick Start

### Create a Zenodo Archive

```bash
# Full archive (all years, all tables) - WARNING: Very large!
python prepare_zenodo_archive.py --years all --output maude_full_archive

# Recent years (recommended for most use cases)
python prepare_zenodo_archive.py --years 2015-2024 --output maude_2015_2024

# Specific device data only
python prepare_zenodo_archive.py --years 2020-2024 --tables device master --output maude_devices_2020_2024

# With compression
python prepare_zenodo_archive.py --years 2020-2024 --compress --output maude_2020_2024
```

### Check FDA Site Compatibility

```bash
# Full check (downloads 1998 data to test, ~3MB)
python check_fda_compatibility.py

# Quick check (no downloads, just HEAD requests)
python check_fda_compatibility.py --quick

# JSON output (for automation)
python check_fda_compatibility.py --json
```

## Archive Contents

After running `prepare_zenodo_archive.py`, your output directory contains:

```
maude_archive/
├── maude_archive.db        # SQLite database with all data
├── README.md               # Human-readable documentation
├── schema.json            # Complete database schema
├── metadata.json          # Machine-readable metadata
└── checksums.txt          # SHA-256 checksums for verification
```

## Upload to Zenodo

After creating an archive:

1. Go to https://zenodo.org/ and create an account
2. Click "Upload" → "New upload"
3. Upload all files from the archive directory
4. Fill in metadata:
   - **Title**: FDA MAUDE Database Archive (YYYY.MM.DD)
   - **Creators**: Your name and affiliation
   - **Description**: Copy from generated README.md
   - **Keywords**: FDA, MAUDE, medical devices, adverse events
   - **License**: Creative Commons Zero v1.0 Universal (Public Domain)
5. Add related identifier: https://www.accessdata.fda.gov/MAUDE/ftparea (isSourceOf)
6. Click "Publish"

Zenodo will assign a DOI for permanent citation.

## GitHub Badge Setup

To add automatic compatibility monitoring to your GitHub repository:

### Prerequisites
- GitHub repository with PyMAUDE code
- GitHub account

### Setup Steps

1. **Create GitHub Gist**
   - Go to https://gist.github.com/
   - Create a new **secret** gist
   - Filename: `pymaude_fda_compatibility.json`
   - Content:
     ```json
     {
       "schemaVersion": 1,
       "label": "FDA Site",
       "message": "checking...",
       "color": "yellow"
     }
     ```
   - Note the Gist ID from the URL

2. **Create Personal Access Token**
   - Go to https://github.com/settings/tokens
   - Generate new token (classic)
   - Scopes: `gist` only
   - Copy the token

3. **Add Repository Secret**
   - Your repo → Settings → Secrets → Actions
   - New secret: `GIST_SECRET`
   - Value: Your personal access token

4. **Update Workflow**
   - Edit `.github/workflows/fda_compatibility_check.yml`
   - Replace `YOUR_GIST_ID_HERE` with your Gist ID
   - Commit and push

5. **Update README Badge**
   - Replace the placeholder badge with:
     ```markdown
     [![FDA Site Compatibility](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/USERNAME/GIST_ID/raw/pymaude_fda_compatibility.json)](https://github.com/jhschwartz/PyMAUDE/actions/workflows/fda_compatibility_check.yml)
     ```

See [docs/github_badge_setup.md](docs/github_badge_setup.md) for detailed instructions.

## Compatibility Check Details

The compatibility checker verifies:

1. ✓ FDA base URL is accessible
2. ✓ File naming patterns match expectations
3. ✓ Files are available for download
4. ✓ Downloaded files can be parsed correctly

### Exit Codes
- `0` - Compatible
- `1` - Incompatible (breaking changes detected)

### What to Do If Incompatible

If the compatibility check fails:

1. **Review the error messages** - Run with `--json` for structured output
2. **Check the FDA website** - Visit https://www.accessdata.fda.gov/MAUDE/ftparea
3. **Create a Zenodo archive** - Preserve current data before FDA changes take effect
4. **Update the code** - Modify `pymaude.py` to handle new patterns
5. **Document changes** - Update documentation with new URL patterns
6. **Notify users** - If you have users of your library, let them know

## Automated Monitoring

Once GitHub Actions is set up:

- **Daily checks** at 6 AM UTC
- **Auto-creates issues** if incompatible
- **Auto-closes issues** when fixed
- **Badge updates** automatically
- **Artifacts saved** for 90 days

## Best Practices

### For Active Research

1. **Create periodic archives** (e.g., annually)
2. **Monitor compatibility** via GitHub badge
3. **Keep raw downloaded files** in case you need to re-process
4. **Document your analysis** with specific data versions

### For Publications

1. **Create archive before analysis**
2. **Upload to Zenodo** and get DOI
3. **Cite the specific archive** in your paper
4. **Include data version** in methods section

Example citation:
```
Schwartz, J. (2024). FDA MAUDE Database Archive (2024.12.29) [Data set].
Zenodo. https://doi.org/10.5281/zenodo.XXXXXX
```

## Disk Space Requirements

Approximate sizes (uncompressed):

- **Full archive (1991-2024)**: ~50-100 GB
- **Recent 5 years (2020-2024)**: ~10-20 GB
- **Device table only (2020-2024)**: ~2-5 GB
- **Single year (device+master)**: ~1-2 GB

Compression with gzip typically reduces size by 60-80%.

## Troubleshooting

### Archive script fails

```bash
# Check dependencies
pip install pandas requests

# Use existing data (don't re-download)
python prepare_zenodo_archive.py --years 2020-2024 --no-download
```

### Compatibility check fails

```bash
# Try quick mode first
python check_fda_compatibility.py --quick

# Check your internet connection
ping www.accessdata.fda.gov

# Review detailed errors
python check_fda_compatibility.py --json | python -m json.tool
```

### GitHub Action not running

1. Check that workflow file is in `.github/workflows/`
2. Verify GitHub Actions is enabled in repo settings
3. Check Action logs for errors
4. Ensure secrets are set correctly

## Support

For issues:
- **Documentation bugs**: Open issue on GitHub
- **FDA website changes**: Run compatibility check and create archive
- **Zenodo help**: See https://help.zenodo.org/

## Related Files

- [prepare_zenodo_archive.py](prepare_zenodo_archive.py) - Archive creation script
- [check_fda_compatibility.py](check_fda_compatibility.py) - Compatibility checker
- [docs/github_badge_setup.md](../docs/github_badge_setup.md) - Detailed badge setup guide
- [.github/workflows/fda_compatibility_check.yml](../.github/workflows/fda_compatibility_check.yml) - GitHub Actions workflow
