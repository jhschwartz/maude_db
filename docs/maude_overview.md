# MAUDE Database Overview

This document provides background on the FDA MAUDE database for researchers using the `maude_db` library.

## What is MAUDE?

MAUDE (Manufacturer and User Facility Device Experience) is the FDA's database for medical device adverse event reports. It contains reports of device malfunctions, injuries, and deaths associated with medical devices.

The database is maintained by the FDA's Center for Devices and Radiological Health (CDRH) and is publicly accessible under the Freedom of Information Act (FOI).

For detailed regulatory information, see the [official FDA MAUDE documentation](https://www.fda.gov/medical-devices/mandatory-reporting-requirements-manufacturers-importers-and-device-user-facilities/manufacturer-and-user-facility-device-experience-database-maude).

## Data Sources

MAUDE contains adverse event reports from three primary sources:

1. **Mandatory Manufacturer Reports**: Device manufacturers must report deaths, serious injuries, and malfunctions
2. **Voluntary Reports**: Healthcare facilities, patients, and caregivers may submit reports
3. **User Facility Reports**: Hospitals and nursing homes must report deaths and serious injuries

The FDA updates MAUDE monthly with new reports.

## MAUDE Table Structure

The MAUDE database is organized into several related tables. The `maude_db` library supports the most commonly used tables:

### Master Table (MDRFOI)

**Purpose**: Core event-level information

**Key columns**:
- `MDR_REPORT_KEY` - Unique identifier for each adverse event report
- `DATE_RECEIVED` - When FDA received the report
- `EVENT_TYPE` - Type of event (Death, Injury, Malfunction, or combinations)
- `MANUFACTURER_NAME` - Name of device manufacturer
- `REPORT_SOURCE_CODE` - Source of report (manufacturer, user facility, etc.)

**Availability**: Only available as cumulative files:
- Historical data: `mdrfoithru2024.zip` (all data through previous year)
- Current year: `mdrfoi.zip` (current year data only)

**Note**: The library automatically uses batch processing to efficiently extract requested years from the cumulative file in a single pass, providing ~29x speedup compared to naive year-by-year processing.

### Device Table (FOIDEV)

**Purpose**: Device-specific information for each report

**Key columns**:
- `MDR_REPORT_KEY` - Links to master table (note: **uppercase** in actual data)
- `DEVICE_REPORT_PRODUCT_CODE` - FDA product code identifying device type
- `GENERIC_NAME` - Generic device name
- `BRAND_NAME` - Brand/trade name of device
- `MANUFACTURER_D_NAME` - Device manufacturer
- `DEVICE_SEQUENCE_NUMBER` - Multiple devices can be involved in one event

**Availability**: Individual year files from 1998-present (`foidev[year].zip`)

### Text Table (FOITEXT)

**Purpose**: Narrative descriptions of adverse events

**Key columns**:
- `MDR_REPORT_KEY` - Links to master table (**uppercase**)
- `MDR_TEXT_KEY` - Identifier for this text record
- `TEXT_TYPE_CODE` - Type of narrative (D=description, E=evaluation, etc.)
- `FOI_TEXT` - Actual narrative text describing the event

**Availability**: Individual year files from 1996-present (`foitext[year].zip`)

### Patient Table (PATIENT)

**Purpose**: Patient demographic information

**Key columns**:
- `MDR_REPORT_KEY` - Links to master table
- `PATIENT_SEQUENCE_NUMBER` - Multiple patients can be involved in one event
- `DATE_OF_EVENT` - When adverse event occurred
- `SEQUENCE_NUMBER_TREATMENT` - Treatment information
- `SEQUENCE_NUMBER_OUTCOME` - Patient outcome codes

**Availability**: Only available as cumulative files:
- Historical data: `patientthru2024.zip` (all data through previous year)
- Current year: `patient.zip` (current year data only)

Patient data is distributed as a single large cumulative file (117MB compressed, 841MB uncompressed) containing all historical records. The library uses batch processing to efficiently filter this file and extract only the requested years in a single pass.

### Device Problem Table (FOIDEVPROBLEM)

**Purpose**: Coded device problem classifications

**Key columns**:
- `MDR_REPORT_KEY` - Links to master table
- `DEVICE_SEQUENCE_NUMBER` - Which device (if multiple)
- `DEVICE_PROBLEM_CODE` - Standardized problem code

**Availability**: Individual year files from 2019-present (recent years only)

## Entity Relationships

```
MASTER (MDR_REPORT_KEY)
  |
  +-- DEVICE (MDR_REPORT_KEY) [1:many]
  |     |
  |     +-- DEVICE_PROBLEM (MDR_REPORT_KEY, DEVICE_SEQUENCE_NUMBER) [1:many]
  |
  +-- TEXT (MDR_REPORT_KEY) [1:many]
  |
  +-- PATIENT (MDR_REPORT_KEY) [1:many]
```

One adverse event report (master) can involve:
- Multiple devices
- Multiple narrative text records
- Multiple patients
- Multiple device problems

## Important: Column Name Case

**Critical for queries**: The actual FDA data files use **UPPERCASE** column names for all tables:

- All tables use uppercase: `MDR_REPORT_KEY`, `DEVICE_REPORT_PRODUCT_CODE`, `GENERIC_NAME`, `BRAND_NAME`, `DATE_RECEIVED`, `EVENT_TYPE`, `FOI_TEXT`, etc.

When writing SQL queries directly, always use uppercase column names:

```python
# Correct - uppercase column names
db.query("SELECT GENERIC_NAME, BRAND_NAME FROM device")
db.query("SELECT EVENT_TYPE, DATE_RECEIVED FROM master")

# Incorrect - will fail
db.query("SELECT generic_name FROM device")  # Error: no such column
db.query("SELECT event_type FROM master")    # Error: no such column
```

The `maude_db` query methods handle joins automatically using the correct case.

## Data Availability by Year

| Table | Supported Years | File Pattern | Notes |
|-------|----------------|--------------|-------|
| Master (MDRFOI) | 1991-present | `mdrfoithru[year].zip` | Cumulative file only (~150MB), filtered by year automatically |
| Device (FOIDEV) | **1998-present** | `foidev[year].zip` (1998-1999)<br>`device[year].zip` (2000-2024)<br>`device.zip` (2025) | Note: naming convention changed in 2000 |
| Text (FOITEXT) | **1996-present** | `foitext[year].zip` | ~45MB per year |
| Patient | **1996-present** | `patientthru[year].zip` | Cumulative file only (117MB compressed, 841MB uncompressed), filtered by year automatically |
| Device Problem | **2019-present** | `foidevproblem[year].zip` | Recent years only |

### Current Year Support (2025)

For the current year, files use yearless names:
- `device.zip` instead of `device2025.zip`
- `foitext.zip` instead of `foitext2025.zip`
- `mdrfoi.zip` instead of `mdrfoithru2025.zip`
- `patient.zip` instead of `patientthru2025.zip`

### Legacy Data NOT Supported

The library does **not** support legacy "thru" files that were used before individual year files existed:
- `foidevthru1997.zip` - NOT supported
- `foitextthru1995.zip` - NOT supported

If you need data before the supported year ranges, you would need to manually download and process these legacy files.

### Incremental Updates

The library currently does **not** support monthly incremental update files (`*add.zip`, `*change.zip`). Only full year files are supported. For the most current data, use the current year yearless files (e.g., `device.zip` for 2025).

The `maude_db` library automatically handles the different naming conventions and filters cumulative files to extract only the requested years.

## Understanding FDA Product Codes

Each medical device has a three-letter FDA product code that classifies its type. Examples:

- `NIQ` - Catheter, Intravascular, Therapeutic, Short-term Less Than 30 Days
- `DQY` - Pacemaker, Implantable
- `DSM` - Stent, Coronary, Drug-Eluting

**Looking up product codes**:
- [FDA Product Classification Database](https://www.fda.gov/medical-devices/classify-your-medical-device/product-classification)
- Use `DEVICE_REPORT_PRODUCT_CODE` column in device table

Product codes are useful for precise device queries:

```python
# More precise than searching by name
devices = db.query_device(product_code='NIQ')
```

## Data Quality Considerations

### Reporting Biases

- **Voluntary reporting**: Not all adverse events are reported
- **Publicity effect**: High-profile device problems may increase reporting
- **Regulatory changes**: Reporting requirements have changed over time
- **Multiple reports**: Same event may generate multiple reports

### Missing Data

- Not all fields are populated in every report
- Narratives may be redacted to protect patient privacy
- Some manufacturers provide more complete data than others

### Using MAUDE Data Responsibly

- MAUDE data **cannot** establish causation between device and adverse event
- Reports are **unverified** - they represent allegations, not confirmed facts
- Use for **signal detection** and **hypothesis generation**, not definitive conclusions
- Always consider denominator (devices in use) when interpreting event counts

## FDA Resources

- **MAUDE Web Interface**: [https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/search.cfm](https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/search.cfm)
- **Data Files**: [https://www.fda.gov/medical-devices/mandatory-reporting-requirements-manufacturers-importers-and-device-user-facilities/manufacturer-and-user-facility-device-experience-database-maude](https://www.fda.gov/medical-devices/mandatory-reporting-requirements-manufacturers-importers-and-device-user-facilities/manufacturer-and-user-facility-device-experience-database-maude)
- **Product Code Database**: [https://www.fda.gov/medical-devices/classify-your-medical-device/product-classification](https://www.fda.gov/medical-devices/classify-your-medical-device/product-classification)
- **File Format Documentation**: Included in ZIP downloads from FDA

---

**Next**: See [getting_started.md](getting_started.md) for hands-on tutorial using `maude_db`.