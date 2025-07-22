import argparse
import calendar
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from .database_manager import DatabaseManager
from .geoserver_client import GeoServerClient
from .tools.logging_manager import error, info, warning

GEOSERVER_CONFIG_NAME = (
    "location_etl_geoserver_config"  # Name of the data source in database
)


try:
    from .data_aggregator import DataAggregator
except ImportError:

    class DataAggregator:
        def __init__(self):
            warning(
                "DataAggregator module not available, using placeholder",
                component="main",
            )

        def calculate_monthly_aggregations(self, *args, **kwargs):
            warning(
                "DataAggregator not available - monthly aggregations skipped",
                component="main",
            )
            return {}


# Example usage:
# python -m src.aclimate_v3_historical_location_etl.aclimate_run_etl \
#   --start_date 2025-04 --end_date 2025-04 --country HONDURAS --all_locations


def parse_args():
    """Parse command line arguments for historical location ETL."""
    info("Parsing command line arguments", component="setup")
    parser = argparse.ArgumentParser(
        description="Historical Location Climate Data ETL Pipeline"
    )

    # Required arguments
    parser.add_argument("--country", required=True, help="Country name for processing")
    parser.add_argument(
        "--start_date", required=True, help="Start date in YYYY-MM format"
    )
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM format")

    # Location selection (mutually exclusive)
    location_group = parser.add_mutually_exclusive_group()
    location_group.add_argument(
        "--location_ids", help="Comma-separated list of location IDs (e.g., 1,2,3,4)"
    )
    location_group.add_argument(
        "--all_locations",
        action="store_true",
        help="Process all locations from database",
    )

    # Pipeline control flags
    parser.add_argument(
        "--data_path", help="Base directory for temporary data (optional)"
    )

    args = parser.parse_args()

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


def validate_dates(start_date: str, end_date: str):
    """Validate date format and range, and convert to full date range."""
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
        sys.exit(1)


def setup_directories(data_path: Optional[str], country: str) -> Dict[str, Path]:
    """Create necessary directory structure if data_path is provided."""
    if not data_path:
        return {}

    info(
        "Setting up directory structure",
        component="setup",
        data_path=data_path,
        country=country,
    )

    base_path = Path(data_path)
    directories = {
        "base": base_path,
        "country": base_path / country.lower(),
        "raw": base_path / country.lower() / "raw",
        "processed": base_path / country.lower() / "processed",
        "temp": base_path / country.lower() / "temp",
    }

    for name, path in directories.items():
        path.mkdir(parents=True, exist_ok=True)
        info(f"Created directory: {name}", component="setup", path=str(path))

    return directories


def cleanup_temp_files(directories: Dict[str, Path]):
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


def main():
    """Main ETL pipeline execution."""
    try:
        info("Starting Historical Location ETL Pipeline", component="main")

        # Parse arguments
        args = parse_args()

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

        # Validate and convert dates first
        start_date_actual, end_date_actual = validate_dates(
            args.start_date, args.end_date
        )

        # Initialize database manager and get configuration
        geoserver_config = db_manager.get_geoserver_config(
            GEOSERVER_CONFIG_NAME, args.country
        )
        geoserver_client = GeoServerClient(geoserver_config)

        # Extract data from GeoServer (validation included)
        data = geoserver_client.extract_location_data(
            location_ids=args.location_ids if args.location_ids else "all",
            country=args.country,
            start_date=start_date_actual,
            end_date=end_date_actual,
        )

        # Save extracted data to database
        save_success = db_manager.save_extracted_data(
            data, args.country, geoserver_config
        )
        if not save_success:
            warning(
                "Some errors occurred while saving data to database", component="main"
            )
        else:
            info("All extracted data saved successfully to database", component="main")

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
            monthly_save_success = db_manager.save_monthly_data(
                monthly_data, args.country, geoserver_config
            )
            if not monthly_save_success:
                warning(
                    "Some errors occurred while saving monthly data to database",
                    component="main",
                )
            else:
                info(
                    "All monthly aggregations saved successfully to database",
                    component="main",
                )

        info(
            "ETL Pipeline completed successfully",
            component="main",
            country=args.country,
            total_records=len(data),
            monthly_records=len(monthly_data) if not monthly_data.empty else 0,
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
