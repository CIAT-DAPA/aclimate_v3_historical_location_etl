# AClimate V3 Historical Location ETL ⛅️📦

## 🏷️ Version & Tags

![GitHub release (latest by date)](https://img.shields.io/github/v/release/CIAT-DAPA/aclimate_v3_historical_location_etl)
![GitHub tag (latest by date)](https://img.shields.io/github/v/tag/CIAT-DAPA/aclimate_v3_historical_location_etl)

**Tags:** `climate-data`, `etl`, `locations`, `python`, `geoserver`, `historical`, `orm`, `data-processing`

## 📌 Introduction

Python package for processing historical climate data for locations with a complete ETL pipeline that includes:

- Data extraction from GeoServer, CSV files, or database
- Monthly aggregation and climatology calculations
- ORM integration for database operations
- Structured logging and OpenTelemetry monitoring

**Key Features:**

- Automated processing of temperature, precipitation, and solar radiation data
- Flexible configuration for multiple countries and locations
- End-to-end pipeline from raw data to database
- Database-backed configuration management
- Multiple data sources: GeoServer or CSV import

---

## Documentation

For complete documentation, visit the [Project Wiki](https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl/wiki)

---

## Features

- Modular ETL pipeline for climate data
- Integration with `aclimate_v3_orm` for database operations
- Typed schemas for all data
- OpenTelemetry monitoring (SigNoz ready)
- CI/CD pipeline (GitHub Actions)
- Structured logging for debugging and monitoring
- Flexible configuration via environment and .env files

---

## ✅ Prerequisites

- Python >= 3.10
- GeoServer
- PostgreSQL database for configuration storage
- Dependencies: see `pyproject.toml` or `requirements.txt`

---

## ⚙️ Installation

```bash
pip install git+https://github.com/CIAT-DAPA/aclimate_v3_orm
pip install git+https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl.git
```

To install a specific version:

```bash
pip install git+https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl.git@v0.1.0
```

For development (editable mode):

```bash
pip install -e .[dev]
```

## 📁 Project files included

- `requirements.txt` (all dependencies)
- `pyproject.toml` (project metadata and dependencies)
- `dev.py` (utility script for setup, test, build, etc.)
- `.env.example` (example environment variables)

## 🔧 Configuration

### Environment Variables

You can configure the ETL by creating a `.env` file or setting environment variables:

```ini
GEOSERVER_URL=http://localhost:8080/geoserver/rest/
GEOSERVER_USER=admin
GEOSERVER_PASSWORD=admin
ENABLE_SIGNOZ=false
OTLP_ENDPOINT=localhost:4317
LOG_FILE_PATH=application.log
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/database
```

> [!NOTE]
> Options:
>
> - `GEOSERVER_URL`: Geoserver URL
> - `GEOSERVER_USER`: Geoserver user
> - `GEOSERVER_PASSWORD`: Geoserver password
> - `OTLP_ENDPOINT`: Signoz endpoint to send logs
> - `ENABLE_SIGNOZ`: Flag to send logs to signoz
> - `LOG_FILE_PATH`: Path to save logs
> - `DATABASE_URL`: Connection string to database

## 🚀 Basic Usage

### 1. Command Line Interface

#### GeoServer Mode (default)

```bash
python -m aclimate_v3_historical_location_etl.aclimate_run_etl \
  --country HONDURAS \
  --start_date 2025-04 \
  --end_date 2025-04 \
  --all_locations \
  --source geoserver
```

#### CSV Import Mode

```bash
# Path to directory containing multiple CSV files (tmax_daily_data.csv, tmin_daily_data.csv, etc.)
python -m aclimate_v3_historical_location_etl.aclimate_run_etl \
  --country HONDURAS \
  --start_date 2025-04 \
  --end_date 2025-04 \
  --all_locations \
  --source csv \
  --csv_path /path/to/csv_directory/

# Or path to a single CSV file
python -m aclimate_v3_historical_location_etl.aclimate_run_etl \
  --country HONDURAS \
  --start_date 2025-04 \
  --end_date 2025-04 \
  --all_locations \
  --source csv \
  --csv_path /path/to/single_file.csv
```

#### Process Specific Locations

```bash
# GeoServer mode
python -m aclimate_v3_historical_location_etl.aclimate_run_etl \
  --country HONDURAS \
  --start_date 2025-04 \
  --end_date 2025-04 \
  --location_ids 1,2,3,4 \
  --source geoserver

# CSV mode (directory with multiple variable CSVs)
python -m aclimate_v3_historical_location_etl.aclimate_run_etl \
  --country HONDURAS \
  --start_date 2025-04 \
  --end_date 2025-04 \
  --location_ids 1,2,3,4 \
  --source csv \
  --csv_path /path/to/csv_directory/
```

> [!NOTE]
> Options:
>
> - `--source`: Data source (`geoserver` or `csv`, default: `geoserver`)
> - `--csv_path`: Path to CSV file (required when `--source csv`)
> - `--location_ids`: Comma-separated list of location IDs
> - `--all_locations`: Process all locations (from database for geoserver, from CSV for csv)
> - `--climatology`: Calculate monthly climatology

> [!IMPORTANT] > **Location Selection Behavior:**
>
> - **GeoServer mode with `--all_locations`**: Processes all locations from database
> - **CSV mode with `--all_locations`**: Processes all locations found in the CSV file
> - **CSV mode with `--location_ids`**: Validates IDs exist in database, then filters CSV data
>
> **CSV Format:** Files should be named `(variable)_daily_data.csv` (e.g., `tmax_daily_data.csv`, `prec_daily_data.csv`) with columns: `ext_id` (or `id`), `day`, `month`, `year`, `value`. Uses PyArrow engine for fast reading.

### 2. Programmatic Usage

```python
from aclimate_v3_historical_location_etl.aclimate_run_etl import main as run_etl_pipeline

run_etl_pipeline()
```

## 🧪 Running Tests

```bash
# Install test requirements
pip install pytest pytest-mock

# Run tests
pytest tests/
```

## 🔄 CI/CD Pipeline Overview

Our GitHub Actions pipeline implements a three-stage deployment process:

```bash
Code Push → Test Stage → Merge Stage → Release Stage
```

### 1. Test & Validate Phase

- Linting with flake8
- Formatting with black
- Type checking with mypy
- Tests with pytest + coverage
- Upload to Codecov

### 2. Merge Phase

- Auto-merges `stage` → `main` after successful tests

### 3. Release Phase

- Creates versioned release and uploads artifacts

## 📊 Project Structure

```bash
aclimate_v3_historical_location_etl/
│
├── .github/
│   └── workflows/
├── src/
│   └── aclimate_v3_historical_location_etl/
│       ├── climate_processing/
│       │   ├── climatology_calculator.py
│       │   └── data_aggregator.py
│       ├── data_managment/
│       │   ├── csv_client.py          # NEW: CSV import functionality
│       │   ├── data_validator.py
│       │   ├── database_manager.py
│       │   └── geoserver_client.py
│       ├── tools/
│       │   ├── logging_manager.py
│       │   └── tools.py
│       ├── aclimate_run_etl.py
│       └── __init__.py
├── tests/
├── requirements.txt
├── pyproject.toml
├── dev.py
├── .env.example
└── README.md
```

## Badges

![Release](https://github.com/CIAT-DAPA/aclimate_v3_historical_location_etl/workflows/Release%20and%20Deploy/badge.svg)
