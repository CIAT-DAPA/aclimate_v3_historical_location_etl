import argparse
from pathlib import Path
from datetime import datetime, date
import calendar
from typing import Dict, List, Union, Any, Optional
import sys
from .tools.logging_manager import info, warning, error
from .database_manager import DatabaseManager
from .geoserver_client import GeoServerClient


GEOSERVER_CONFIG_NAME = "location_etl_geoserver_config"  # Name of the data source in database


try:
    from .data_aggregator import DataAggregator
except ImportError:
    class DataAggregator:
        def __init__(self):
            warning("DataAggregator module not available, using placeholder", 
                   component="main")
        def calculate_monthly_aggregations(self, *args, **kwargs):
            warning("DataAggregator not available - monthly aggregations skipped", 
                   component="main")
            return {}

# python -m src.aclimate_v3_historical_location_etl.aclimate_run_etl --start_date 2025-04 --end_date 2025-04 --country HONDURAS --all_locations

def parse_args():
    """Parse command line arguments for historical location ETL."""
    info("Parsing command line arguments", component="setup")
    parser = argparse.ArgumentParser(description="Historical Location Climate Data ETL Pipeline")
    
    # Required arguments
    parser.add_argument("--country", required=True, help="Country name for processing")
    parser.add_argument("--start_date", required=True, help="Start date in YYYY-MM format")
    parser.add_argument("--end_date", required=True, help="End date in YYYY-MM format")
    
    # Location selection (mutually exclusive)
    location_group = parser.add_mutually_exclusive_group()
    location_group.add_argument("--location_ids", 
                              help="Comma-separated list of location IDs (e.g., 1,2,3,4)")
    location_group.add_argument("--all_locations", action="store_true",
                              help="Process all locations from database")
    
    # Pipeline control flags
    parser.add_argument("--data_path", help="Base directory for temporary data (optional)")
    
    args = parser.parse_args()
    
    # Default to all locations if no location selection specified
    if not args.location_ids and not args.all_locations:
        args.all_locations = True
        info("No location selection specified, defaulting to all locations", component="setup")
    
    info("Command line arguments parsed successfully", 
         component="setup",
         args=vars(args))
    return args

def validate_dates(start_date: str, end_date: str):
    """Validate date format and range, and convert to full date range."""
    try:
        info("Validating date range", 
             component="validation",
             start_date=start_date,
             end_date=end_date)
        
        start_month = datetime.strptime(start_date, "%Y-%m")
        end_month = datetime.strptime(end_date, "%Y-%m")
        if start_month > end_month:
            raise ValueError("Start date must be before end date")
        
        # Convert to actual date range: first day of start month to last day of end month
        start_actual = start_month.replace(day=1)
        last_day = calendar.monthrange(end_month.year, end_month.month)[1]
        end_actual = end_month.replace(day=last_day)
            
        info("Date validation successful", 
             component="validation",
             actual_start=start_actual.strftime('%Y-%m-%d'),
             actual_end=end_actual.strftime('%Y-%m-%d'))
        return start_actual, end_actual
    except ValueError as e:
        error("Invalid date format", 
              component="validation",
              error=str(e))
        print(f"ERROR: Invalid date format. Use YYYY-MM. Error: {str(e)}")
        sys.exit(1)
        sys.exit(1)

def setup_directories(data_path: Optional[str], country: str) -> Dict[str, Path]:
    """Create necessary directory structure if data_path is provided."""
    if not data_path:
        return {}
        
    info("Setting up directory structure", 
         component="setup",
         data_path=data_path,
         country=country)
    
    base_path = Path(data_path)
    directories = {
        'base': base_path,
        'country': base_path / country.lower(),
        'raw': base_path / country.lower() / 'raw',
        'processed': base_path / country.lower() / 'processed',
        'temp': base_path / country.lower() / 'temp'
    }
    
    for name, path in directories.items():
        path.mkdir(parents=True, exist_ok=True)
        info(f"Created directory: {name}", 
             component="setup",
             path=str(path))
    
    return directories


def cleanup_temp_files(directories: Dict[str, Path]):
    """Clean up temporary files."""
    if not directories:
        return
        
    temp_path = directories.get('temp')
    if temp_path and temp_path.exists():
        try:
            import shutil
            shutil.rmtree(temp_path)
            info("Temporary files cleaned up", 
                 component="cleanup",
                 path=str(temp_path))
        except Exception as e:
            warning("Failed to clean up temporary files", 
                   component="cleanup",
                   error=str(e))

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
            error("Failed to initialize database manager", 
                  component="main",
                  error=str(e))
            print("ERROR: aclimate_v3_orm is required for the ETL pipeline to function.")
            print("Please install it with: pip install aclimate_v3_orm")
            sys.exit(1)
        
        # Validate and convert dates first
        start_date_actual, end_date_actual = validate_dates(args.start_date, args.end_date)

        # Initialize database manager and get configuration
        geoserver_config = db_manager.get_geoserver_config(GEOSERVER_CONFIG_NAME, args.country)
        geoserver_client = GeoServerClient(geoserver_config)

        # Extract data from GeoServer (validation included)
        data = geoserver_client.extract_location_data(
            location_ids=args.location_ids if args.location_ids else "all",
            country=args.country,
            start_date=start_date_actual,
            end_date=end_date_actual
        )
        
        # Save extracted data to database
        save_success = db_manager.save_extracted_data(data, args.country)
        if not save_success:
            warning("Some errors occurred while saving data to database", 
                   component="main")
        else:
            info("All extracted data saved successfully to database", 
                 component="main")
        
        # Data extraction and validation completed successfully
        info("Data extraction and validation completed successfully",
             component="main",
             total_records=len(data))
        return
        # Initialize data aggregator
        data_aggregator = DataAggregator()
        
        # Climatology calculation not implemented yet
        # climatology_calculator = ClimatologyCalculator() if args.climatology else None
        
        # Validate inputs
        start_date, end_date = validate_dates(args.start_date, args.end_date)
        
        # Get locations using database manager
        try:
            if args.location_ids:
                locations = db_manager.get_locations_by_ids(args.location_ids, args.country)
            else:
                locations = db_manager.get_all_locations(args.country)
        except Exception as e:
            error("Failed to retrieve locations", 
                  component="main",
                  error=str(e))
            print(f"ERROR: Failed to retrieve locations: {str(e)}")
            sys.exit(1)
        
        if not locations:
            error("No locations found for processing", 
                  component="main",
                  country=args.country,
                  location_criteria=args.location_ids or "all")
            print("ERROR: No locations found for processing")
            sys.exit(1)
        
        # Setup directories (optional, only if data_path provided)
        directories = setup_directories(args.data_path, args.country)
        
        # Variables configuration is now handled in GeoServer client
        info("ETL Pipeline configuration", 
             component="main",
             country=args.country,
             date_range=f"{args.start_date} to {args.end_date}",
             location_count=len(locations))
        
        # Convert locations to format expected by spatial processor
        locations_list = []
        for location in locations:
            locations_list.append({
                'id': location.id,
                'name': location.name,
                'latitude': location.latitude,
                'longitude': location.longitude
            })
        
        # Main processing loop
        all_daily_data = []
        
        if not args.skip_download:
            # Step 1: Extract location data from GeoServer
            info("Starting location data extraction from GeoServer", 
                 component="main",
                 date_range=f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            try:
                # Determine location IDs for extraction
                location_ids_str = args.location_ids if args.location_ids else "all"
                
                # Extract data using the simplified GeoServer client
                df_extracted = geoserver_client.extract_location_data(
                    location_ids_str,
                    args.country,
                    start_date,
                    end_date
                )
                
                info(f"Extracted {len(df_extracted)} records of location data", 
                     component="main")
                
                all_daily_data = df_extracted
                
                info(f"Processed {len(all_daily_data)} days of point data", 
                     component="main")
                
            except Exception as e:
                error("Failed to download or process spatial data", 
                      component="main",
                      error=str(e))
                print(f"ERROR: Failed to download spatial data: {str(e)}")
                sys.exit(1)
        
        # Step 3: Save daily data to database
        if all_daily_data:
            info("Saving daily data to database", component="main")
            
            for daily_data in all_daily_data:
                try:
                    # Group by location and save
                    for location_id, location_data in daily_data.get('locations', {}).items():
                        # Find the location object
                        location = next((s for s in locations if s.id == location_id), None)
                        if location:
                            processed_data = {
                                'location_id': location_id,
                                'date': daily_data['date'],
                                'data': location_data
                            }
                            
                            # Save to database (this would be implemented in database_manager)
                            # db_manager.save_daily_data(location_id, processed_data)
                            
                except Exception as e:
                    error(f"Failed to save daily data for date {daily_data['date']}", 
                          component="main",
                          error=str(e))
                    continue
        
        # Step 4: Calculate monthly aggregations
        info("Calculating monthly aggregations", component="main")
        
        # Group daily data by month
        monthly_data = {}
        for daily_data in all_daily_data:
            date_obj = datetime.fromisoformat(daily_data['date']).date()
            month_key = f"{date_obj.year}-{date_obj.month:02d}"
            
            if month_key not in monthly_data:
                monthly_data[month_key] = []
            monthly_data[month_key].append(daily_data)
        
        # Calculate aggregations for each month
        for month_key, month_daily_data in monthly_data.items():
            year, month = month_key.split('-')
            year, month = int(year), int(month)
            
            try:
                monthly_aggregations = data_aggregator.calculate_monthly_aggregations(
                    month_daily_data, year, month
                )
                
                # Save monthly aggregations to database
                for location_id, location_monthly in monthly_aggregations.get('locations', {}).items():
                    # Find the location object
                    location = next((s for s in locations if s.id == location_id), None)
                    if location:
                        # Save to database (this would be implemented in database_manager)
                        # db_manager.save_monthly_data(location_id, location_monthly)
                        pass
                
                info(f"Monthly aggregations calculated for {year}-{month:02d}", 
                     component="main",
                     year=year,
                     month=month,
                     locations_processed=len(monthly_aggregations.get('locations', {})))
                
            except Exception as e:
                error(f"Failed to calculate monthly aggregations for {month_key}", 
                      component="main",
                      error=str(e))
                continue
        
        # Step 5: Calculate climatology (not implemented yet)
        # TODO: Implement climatology calculation when ClimatologyCalculator is available
        
        # Cleanup temporary files
        cleanup_temp_files(directories)
        
        info("ETL Pipeline completed successfully", 
             component="main",
             country=args.country,
             locations_processed=len(locations),
             days_processed=len(all_daily_data) if isinstance(all_daily_data, list) else len(all_daily_data))
        
    except KeyboardInterrupt:
        warning("Pipeline interrupted by user", component="main")
        print("Pipeline interrupted by user")
        sys.exit(1)
    except Exception as e:
        error("Unexpected error in ETL Pipeline", 
              component="main",
              error=str(e))
        sys.exit(1)

if __name__ == "__main__":
    main()