import argparse
import calendar
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd

from .climate_processing.climatology_calculator import ClimatologyCalculator
from .climate_processing.indicators_processor import IndicatorsProcessor
from .data_managment import CSVClient, DatabaseManager, GeoServerClient
from .tools.logging_manager import error, info, warning

GEOSERVER_CONFIG_NAME = "location_etl_geoserver_config"


try:
    from .climate_processing.data_aggregator import DataAggregator
except ImportError:

    class DataAggregator:  # type: ignore[no-redef]
        def __init__(self) -> None:
            warning(
                "DataAggregator module not available, using placeholder",
                component="main",
            )

        def calculate_monthly_aggregations(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            warning(
                "DataAggregator not available - monthly aggregations skipped",
                component="main",
            )
            return {}


# Example usage:
# GeoServer mode:
# python -m src.aclimate_v3_historical_location_etl.aclimate_run_etl \
#   --date_range 2025-04 2025-04 --country HONDURAS --all_locations --source geoserver
#
# CSV mode with specific dates:
# python -m src.aclimate_v3_historical_location_etl.aclimate_run_etl \
#   --date_range 2025-04 2025-04 --country HONDURAS --all_locations --source csv --csv_path path\data_test
#
# CSV mode with all dates:
# python -m src.aclimate_v3_historical_location_etl.aclimate_run_etl \
#   --all_dates --country HONDURAS --all_locations --source csv --csv_path path\data_test
#
# Indicators only (skip data ingestion, explicit year range):
# python -m src.aclimate_v3_historical_location_etl.aclimate_run_etl \
#   --country HONDURAS --indicators --skip_processing --indicator_years 2000-2020
#
# Indicators only (skip data ingestion, reuse date_range years):
# python -m src.aclimate_v3_historical_location_etl.aclimate_run_etl \
#   --country HONDURAS --date_range 2000-01 2020-12 --indicators --skip_processing
#
# Full pipeline with indicators appended (ingest + indicators):
# python -m src.aclimate_v3_historical_location_etl.aclimate_run_etl \
#   --date_range 2025-01 2025-12 --country HONDURAS --all_locations --source geoserver \
#   --indicators --indicator_years 2000-2020


def parse_args():  # type: ignore[no-untyped-def]
    """Parse command line arguments for historical location ETL."""
    info("Parsing command line arguments", component="setup")
    parser = argparse.ArgumentParser(
        description="Historical Location Climate Data ETL Pipeline"
    )

    # Required arguments
    parser.add_argument("--country", required=True, help="Country name for processing")

    # Date arguments (either date range or all dates)
    parser.add_argument(
        "--date_range",
        nargs=2,
        metavar=("START_DATE", "END_DATE"),
        help="Date range in YYYY-MM format (e.g., --date_range 2025-01 2025-12)",
    )
    parser.add_argument(
        "--all_dates",
        action="store_true",
        help="Use all dates available in the data source (CSV only)",
    )

    # Data source selection
    parser.add_argument(
        "--source",
        choices=["geoserver", "csv"],
        default="geoserver",
        help="Data source: 'geoserver' or 'csv' (default: geoserver)",
    )
    parser.add_argument(
        "--csv_path",
        help="Path to CSV file (required when --source csv is used)",
    )

    # Location selection (mutually exclusive)
    location_group = parser.add_mutually_exclusive_group()
    location_group.add_argument(
        "--location_ids", help="Comma-separated list of location IDs (e.g., 1,2,3,4)"
    )
    location_group.add_argument(
        "--all_locations",
        action="store_true",
        help="Process all locations (from database for geoserver, from CSV for csv source)",
    )

    # Pipeline control flags
    parser.add_argument(
        "--climatology",
        action="store_true",
        help="Calculate and save monthly climatology for processed stations",
    )
    parser.add_argument(
        "--indicators",
        action="store_true",
        help="Calculate and save location climate indicators for the country",
    )
    parser.add_argument(
        "--indicator_years",
        metavar="YYYY-YYYY",
        help="Year range for indicator calculation, e.g. '2000-2020'. "
        "If omitted, the --date_range years are used.",
    )
    parser.add_argument(
        "--skip_processing",
        action="store_true",
        help="Skip daily/monthly data ingestion and jump directly to indicators. "
        "Requires --indicators and a date range.",
    )

    args = parser.parse_args()

    # --skip_processing requires --indicators
    if args.skip_processing and not args.indicators:
        parser.error("--skip_processing requires --indicators to be set")

    # Validate date arguments - required unless skipping processing with indicator_years
    if not args.skip_processing or not args.indicator_years:
        if bool(args.date_range) == bool(args.all_dates):
            if not args.date_range and not args.all_dates:
                parser.error("Either --date_range or --all_dates must be specified")
            else:
                parser.error("Cannot specify both --date_range and --all_dates")

    # When skipping processing, a date range must still be available
    if args.skip_processing and not args.date_range and not args.indicator_years:
        parser.error(
            "--skip_processing requires either --date_range or --indicator_years"
        )

    # Source/CSV validations only apply when actually processing data
    if not args.skip_processing:
        # Validate CSV path is provided when source is CSV
        if args.source == "csv" and not args.csv_path:
            error("CSV path is required when using CSV source", component="setup")
            parser.error("--csv_path is required when --source csv is used")

        # Validate all_dates flag is only used with CSV source
        if args.all_dates and args.source != "csv":
            error(
                "--all_dates flag can only be used with CSV source", component="setup"
            )
            parser.error("--all_dates flag is only available when --source csv is used")

    # Validate --indicator_years format when provided
    if args.indicator_years:
        parts = args.indicator_years.split("-")
        if len(parts) != 2 or not all(p.isdigit() and len(p) == 4 for p in parts):
            parser.error(
                "--indicator_years must be in YYYY-YYYY format, e.g. '2000-2020'"
            )
        if int(parts[0]) > int(parts[1]):
            parser.error("--indicator_years start year must be <= end year")

    # Default to all locations if no location selection specified
    if not args.location_ids and not args.all_locations:
        args.all_locations = True
        info(
            "No location selection specified, defaulting to all locations",
            component="setup",
        )

    info(
        "Command line arguments parsed successfully", component="setup", args=vars(args)
    )
    return args


def _run_indicators(
    country: str,
    start_date: str,
    end_date: str,
) -> None:
    """
    Instantiate and run the IndicatorsProcessor for the given country and date range.

    Args:
        country: Country name (uppercase).
        start_date: Start date in YYYY-MM format.
        end_date: End date in YYYY-MM format.
    """
    info(
        "Starting indicators calculation",
        component="processing",
        country=country,
        start_date=start_date,
        end_date=end_date,
    )

    indicators_processor = IndicatorsProcessor(
        country=country,
        start_date=start_date,
        end_date=end_date,
    )

    indicators_processor.process_all_indicators()

    available_indicators = indicators_processor.get_available_indicators()
    indicator_names = [ind.get("short_name", "Unknown") for ind in available_indicators]

    info(
        f"Indicators calculation completed: {indicator_names}",
        component="processing",
        country=country,
        indicators_count=len(available_indicators),
        indicators=indicator_names,
    )


def validate_dates(date_range=None, all_dates=False):  # type: ignore[no-untyped-def]
    """Validate date format and range, and convert to full date range.

    Args:
        date_range: Tuple of (start_date, end_date) strings in YYYY-MM format, or None
        all_dates: Boolean indicating if all dates should be used

    Returns:
        Tuple of (start_date, end_date) datetime objects, or (None, None) if all_dates=True
    """
    if all_dates:
        info("Using all dates from data source", component="validation")
        return None, None

    if not date_range or len(date_range) != 2:
        error(
            "Date range must be provided when not using --all_dates",
            component="validation",
        )
        sys.exit(1)

    start_date, end_date = date_range

    try:
        info(
            "Validating date range",
            component="validation",
            start_date=start_date,
            end_date=end_date,
        )

        start_month = datetime.strptime(start_date, "%Y-%m")
        end_month = datetime.strptime(end_date, "%Y-%m")
        if start_month > end_month:
            raise ValueError("Start date must be before end date")

        # Convert to actual date range: first day of start month to last day
        # of end month
        start_actual = start_month.replace(day=1)
        last_day = calendar.monthrange(end_month.year, end_month.month)[1]
        end_actual = end_month.replace(day=last_day)

        info(
            "Date validation successful",
            component="validation",
            actual_start=start_actual.strftime("%Y-%m-%d"),
            actual_end=end_actual.strftime("%Y-%m-%d"),
        )
        return start_actual, end_actual
    except ValueError as e:
        error("Invalid date format", component="validation", error=str(e))
        print(f"ERROR: Invalid date format. Use YYYY-MM. Error: {str(e)}")
        sys.exit(1)


def cleanup_temp_files(directories: Dict[str, Path]) -> None:
    """Clean up temporary files."""
    if not directories:
        return

    temp_path = directories.get("temp")
    if temp_path and temp_path.exists():
        try:
            import shutil

            shutil.rmtree(temp_path)
            info("Temporary files cleaned up", component="cleanup", path=str(temp_path))
        except Exception as e:
            warning(
                "Failed to clean up temporary files", component="cleanup", error=str(e)
            )


def calculate_and_save_climatologies_from_data(
    db_manager: DatabaseManager, data: pd.DataFrame
) -> None:
    """
    Calculate and save/update monthly climatology for
    each station present in the extracted data.
    Uses all historical monthly data from the database
    for each station (no reference period filtering).

    Args:
        db_manager: Instance of DatabaseManager
        to access database and save climatologies.
        data: Extracted data (DataFrame or list of dicts)
        containing at least 'location_id' for each record.

    Returns:
        None. Climatologies are saved/updated in the database for each station.
    """
    info(
        "Starting monthly climatology calculation for extracted stations",
        component="main",
    )
    climatology_calculator = ClimatologyCalculator()

    # Get unique station IDs from the extracted data
    if hasattr(data, "location_id"):
        station_ids = set(int(x) for x in data["location_id"].unique())
    elif hasattr(data, "__getitem__") and hasattr(data, "__len__"):
        # Fallback for list of dicts
        station_ids = set(
            int(row["location_id"]) for row in data if "location_id" in row
        )
    else:
        warning("Could not determine station IDs from data", component="main")
        return

    for station_id in station_ids:
        # Fetch all historical monthly data for the station from the database
        historical_monthly = db_manager.historical_monthly_service.get_by_location_id(
            int(station_id)
        )
        if not historical_monthly:
            warning(
                f"No monthly data found for station {station_id}",
                component="main",
            )
            continue
        # Convert to list of dicts if needed
        if hasattr(historical_monthly[0], "model_dump"):
            historical_monthly = [row.model_dump() for row in historical_monthly]
        else:
            historical_monthly = [dict(row) for row in historical_monthly]
        # Calculate monthly climatology
        climatology = climatology_calculator.calculate_monthly_climatology(
            historical_monthly, station_id
        )
        # Save or update in the database
        db_manager.save_or_update_climatology(station_id, climatology)

        info(
            f"Monthly climatology processed for station {station_id}",
            component="main",
        )


def main() -> None:
    """Main ETL pipeline execution."""
    try:
        info("Starting Historical Location ETL Pipeline", component="main")

        # Parse arguments
        args = parse_args()

        # ------------------------------------------------------------------
        # Resolve indicator date range early (needed even for skip_processing)
        # ------------------------------------------------------------------
        indicator_start_date: Optional[str] = None
        indicator_end_date: Optional[str] = None

        if args.indicators:
            if args.indicator_years:
                start_yr, end_yr = args.indicator_years.split("-")
                indicator_start_date = f"{start_yr}-01"
                indicator_end_date = f"{end_yr}-12"
            elif args.date_range:
                indicator_start_date = args.date_range[0]
                indicator_end_date = args.date_range[1]
            else:
                error(
                    "No date range available for indicators. "
                    "Use --indicator_years or --date_range.",
                    component="main",
                )
                sys.exit(1)

        # ------------------------------------------------------------------
        # Skip data processing if requested
        # ------------------------------------------------------------------
        if args.skip_processing:
            info("Skipping data processing (--skip_processing)", component="main")
            assert indicator_start_date is not None
            assert indicator_end_date is not None
            _run_indicators(args.country, indicator_start_date, indicator_end_date)
            info("Pipeline (indicators only) completed", component="main")
            return

        # Initialize database manager
        try:
            db_manager = DatabaseManager()
        except ImportError as e:
            error(
                "Failed to initialize database manager", component="main", error=str(e)
            )
            print(
                "ERROR: aclimate_v3_orm is required for the ETL pipeline to function."
            )
            print("Please install it with: pip install aclimate_v3_orm")
            sys.exit(1)

        # Validate and convert dates
        start_date_actual, end_date_actual = validate_dates(
            date_range=args.date_range, all_dates=args.all_dates
        )

        # Extract data based on source type
        if args.source == "geoserver":
            info("Using GeoServer as data source", component="main")

            # Get GeoServer configuration
            geoserver_config = db_manager.get_geoserver_config(
                GEOSERVER_CONFIG_NAME, args.country
            )
            if not geoserver_config:
                error(
                    f"GeoServer configuration not found for {args.country}",
                    component="main",
                )
                return

            # Initialize GeoServer client
            geoserver_client = GeoServerClient(geoserver_config)

            # Extract data from GeoServer (validation included)
            data = geoserver_client.extract_location_data(
                location_ids=args.location_ids if args.location_ids else "all",
                country=args.country,
                start_date=start_date_actual,
                end_date=end_date_actual,
            )

            # Save extracted data to database
            info(f"Saving {len(data)} daily records to database...", component="main")
            save_success = db_manager.save_extracted_data(
                data, args.country, geoserver_config
            )

        else:  # args.source == "csv"
            info("Using CSV as data source", component="main", csv_path=args.csv_path)

            # Initialize CSV client
            csv_client = CSVClient()

            # Extract data from CSV (validation included)
            data = csv_client.extract_location_data(
                location_ids=args.location_ids if args.location_ids else "all",
                country=args.country,
                start_date=start_date_actual,
                end_date=end_date_actual,
                csv_path=args.csv_path,
            )

            # Save extracted data to database (no geoserver_config for CSV)
            info(f"Saving {len(data)} daily records to database...", component="main")
            save_success = db_manager.save_extracted_data(
                data, args.country, geoserver_config=None
            )
        # Check save status
        if not save_success:
            warning(
                "Some errors occurred while saving daily data to database",
                component="main",
            )
        else:
            info(
                f"All {len(data)} daily records saved successfully to database",
                component="main",
            )

        # Data extraction and validation completed successfully
        info(
            "Data extraction and validation completed successfully",
            component="main",
            total_records=len(data),
        )

        # Initialize data aggregator for monthly calculations
        data_aggregator = DataAggregator()

        # Calculate monthly aggregations
        monthly_data = data_aggregator.calculate_monthly_aggregations(data)

        # Save monthly data to database
        if not monthly_data.empty:
            # Use geoserver_config only if source is geoserver
            config_to_pass = geoserver_config if args.source == "geoserver" else None
            info(
                f"Saving {len(monthly_data)} monthly records to database...",
                component="main",
            )
            monthly_save_success = db_manager.save_monthly_data(
                monthly_data, args.country, config_to_pass
            )
            if not monthly_save_success:
                warning(
                    "Some errors occurred while saving monthly data to database",
                    component="main",
                )
            else:
                info(
                    f"All {len(monthly_data)} monthly aggregations saved successfully to database",
                    component="main",
                )

        # If the --climatology flag is set, calculate and save
        # climatologies for the stations in the extracted data
        if args.climatology:
            calculate_and_save_climatologies_from_data(db_manager, data)

        # ------------------------------------------------------------------
        # Indicators calculation
        # ------------------------------------------------------------------
        if args.indicators:
            assert indicator_start_date is not None
            assert indicator_end_date is not None
            _run_indicators(args.country, indicator_start_date, indicator_end_date)

        info(
            "ETL Pipeline completed successfully",
            component="main",
            country=args.country,
            data_source=args.source,
            daily_records_processed=len(data),
            daily_records_saved=len(data) if save_success else 0,
            monthly_records_processed=(
                len(monthly_data) if not monthly_data.empty else 0
            ),
            monthly_records_saved=(
                len(monthly_data)
                if not monthly_data.empty and monthly_save_success
                else 0
            ),
        )

    except KeyboardInterrupt:
        warning("Pipeline interrupted by user", component="main")
        sys.exit(1)
    except Exception as e:
        error(
            f"Unexpected error in ETL Pipeline {str(e)}", component="main", error=str(e)
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
