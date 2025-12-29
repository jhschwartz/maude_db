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
- `mdr_report_key` - Unique identifier for each adverse event report
- `date_received` - When FDA received the report
- `event_type` - Type of event (Death, Injury, Malfunction, or combinations)
- `manufacturer_name` - Name of device manufacturer
- `report_source_code` - Source of report (manufacturer, user facility, etc.)

**Availability**: Only available as a comprehensive file (`mdrfoithru[year].zip`). Individual year files are not provided by FDA.

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
- `mdr_report_key` - Links to master table
- `patient_sequence_number` - Multiple patients can be involved in one event
- `date_of_event` - When adverse event occurred
- `sequence_number_treatment` - Treatment information
- `sequence_number_outcome` - Patient outcome codes

**Availability**: Individual year files from 1996-present

### Device Problem Table (FOIDEVPROBLEM)

**Purpose**: Coded device problem classifications

**Key columns**:
- `MDR_REPORT_KEY` - Links to master table
- `DEVICE_SEQUENCE_NUMBER` - Which device (if multiple)
- `DEVICE_PROBLEM_CODE` - Standardized problem code

**Availability**: Available in recent years

## Entity Relationships

```
MASTER (mdr_report_key)
  |
  +-- DEVICE (MDR_REPORT_KEY) [1:many]
  |     |
  |     +-- DEVICE_PROBLEM (MDR_REPORT_KEY, DEVICE_SEQUENCE_NUMBER) [1:many]
  |
  +-- TEXT (MDR_REPORT_KEY) [1:many]
  |
  +-- PATIENT (mdr_report_key) [1:many]
```

One adverse event report (master) can involve:
- Multiple devices
- Multiple narrative text records
- Multiple patients
- Multiple device problems

## Important: Column Name Case

**Critical for queries**: The actual FDA data files use **UPPERCASE** column names for some tables:

- Device table: `MDR_REPORT_KEY`, `DEVICE_REPORT_PRODUCT_CODE`, `GENERIC_NAME`, `BRAND_NAME`
- Text table: `MDR_REPORT_KEY`, `FOI_TEXT`
- Master and patient tables: lowercase column names

When writing SQL queries directly, always use the correct case:

```python
# Correct - uppercase for device table
db.query("SELECT GENERIC_NAME, BRAND_NAME FROM device")

# Incorrect - will fail
db.query("SELECT generic_name FROM device")  # Error: no such column
```

The `maude_db` query methods handle this automatically.

## Data Availability by Year

| Table | File Pattern | Years Available | Individual Files |
|-------|-------------|-----------------|------------------|
| Master (MDRFOI) | `mdrfoithru[year].zip` | 1991-present | No - comprehensive only |
| Device (FOIDEV) | `foidev[year].zip` | 1998-present | Yes |
| Text (FOITEXT) | `foitext[year].zip` | 1996-present | Yes |
| Patient | `patient[year].zip` | 1996-present | Yes |
| Device Problem | `foidevproblem[year].zip` | Recent years | Yes |

**Note**: The master table file (`mdrfoithru[year].zip`) contains all historical data and is very large (>1GB). The `maude_db` library currently does not download this file automatically. Individual year files are only available for device, text, and patient tables.

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