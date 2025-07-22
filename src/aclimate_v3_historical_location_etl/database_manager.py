from typing import List, Dict, Any, Optional
from datetime import datetime
import json
from .tools.logging_manager import info, warning, error

# ORM imports - Required for database operations
try:
    from aclimate_v3_orm.services import MngLocationService, MngDataSourceService, MngCountryService, ClimateHistoricalDailyService, MngClimateMeasureService
    from aclimate_v3_orm.schemas import LocationRead, ClimateHistoricalDailyCreate
except ImportError as e:
    error("ORM (aclimate_v3_orm) is required for database operations", 
          component="database_manager", 
          error=str(e))
    raise ImportError("aclimate_v3_orm is required for database operations")


class DatabaseManager:
    """Handles all database operations for the ETL pipeline."""
    
    def __init__(self):
        """Initialize database manager with required services."""
        self.location_service = MngLocationService()
        self.data_source_service = MngDataSourceService()
        self.country_service = MngCountryService()
        self.historical_data_service = ClimateHistoricalDailyService()
        self.climate_measure_service = MngClimateMeasureService()
        
        info("Database manager initialized", component="database_manager")
    
    def get_locations_by_ids(self, location_ids: str, country: str) -> List[LocationRead]:
        """Get locations by comma-separated IDs."""
        try:
            info("Retrieving locations by IDs", 
                 component="database_manager",
                 location_ids=location_ids,
                 country=country)
            
            ids = [int(id.strip()) for id in location_ids.split(',')]
            locations = []
            
            for location_id in ids:
                location = self.location_service.get_by_id(location_id)
                if location:
                    locations.append(location)
                    info(f"Found location: {location.name}", 
                         component="database_manager",
                         location_id=location_id,
                         location_name=location.name)
                else:
                    warning(f"Location not found: {location_id}", 
                           component="database_manager",
                           location_id=location_id)
            
            if not locations:
                raise ValueError(f"No valid locations found for IDs: {location_ids}")
                
            info(f"Retrieved {len(locations)} locations", 
                 component="database_manager",
                 location_count=len(locations))
            return locations
            
        except ValueError as e:
            error("Invalid location ID format", 
                  component="database_manager",
                  error=str(e))
            raise ValueError(f"Invalid location ID format. Use comma-separated integers. Error: {str(e)}")
        except Exception as e:
            error("Failed to retrieve locations", 
                  component="database_manager",
                  error=str(e))
            raise Exception(f"Failed to retrieve locations: {str(e)}")
    
    def get_all_locations(self, country: str) -> List[LocationRead]:
        """Get all locations from database for the country."""
        try:
            info("Retrieving all locations from database", 
                 component="database_manager",
                 country=country)
            locations = self.location_service.get_by_country_name(country)
            
            info(f"Retrieved {len(locations)} locations from database", 
                 component="database_manager",
                 location_count=len(locations))
            
            if not locations:
                warning(f"No locations found for country: {country}", 
                       component="database_manager",
                       country=country)
            
            return locations
            
        except Exception as e:
            error("Failed to retrieve all locations", 
                  component="database_manager",
                  error=str(e))
            raise Exception(f"Failed to retrieve all locations: {str(e)}")
    
    def save_climatology_data(self, location_id: int, climatology_data: Dict[str, Any]) -> bool:
        """Save climatology data for a location using ORM schemas."""
        try:
            info("Saving climatology data for location", 
                 component="database_manager",
                 location_id=location_id)
            
            # Climatology data comes already formatted from ORM schemas
            # No need for additional object creation or transformation
            
            # TODO: Implement actual climatology saving logic with proper service
            # Example:
            # climatology_service = ClimatologyService()
            # climatology_service.save(climatology_data)  # data is already properly formatted
            
            info("Climatology data saved successfully", 
                 component="database_manager",
                 location_id=location_id)
            
            return True
            
        except Exception as e:
            error("Failed to save climatology data", 
                  component="database_manager",
                  location_id=location_id,
                  error=str(e))
            raise Exception(f"Failed to save climatology data: {str(e)}")
    
    def validate_location_exists(self, location_id: int) -> bool:
        """Validate that a location exists in the database."""
        try:
            location = self.location_service.get_by_id(location_id)
            return location is not None
        except Exception as e:
            error("Failed to validate location existence", 
                  component="database_manager",
                  location_id=location_id,
                  error=str(e))
            return False
    
    def get_location_info(self, location_id: int) -> Optional[LocationRead]:
        """Get location information using ORM schema directly."""
        try:
            # Return the location schema directly - no need for additional object creation
            # The LocationRead schema already contains all necessary information
            # and is properly typed and formatted
            location = self.location_service.get_by_id(location_id)
            
            if location:
                info("Location information retrieved", 
                     component="database_manager",
                     location_id=location_id,
                     location_name=location.name)
            else:
                warning("Location not found", 
                       component="database_manager",
                       location_id=location_id)
            
            return location
            
        except Exception as e:
            error("Failed to get location info", 
                  component="database_manager",
                  location_id=location_id,
                  error=str(e))
            return None
    
    def get_geoserver_config(self, config_name: str, country: str) -> Optional[Dict[str, Any]]:
        """
        Get GeoServer configuration from database by country and name filter.
        
        Args:
            config_name: Name of the configuration data source
            country: Country to filter configurations by (required)
            
        Returns:
            Configuration dictionary or None if not found
        """
        try:
            # Validate that country is provided
            if not country:
                error("Country parameter is required for GeoServer configuration lookup",
                      component="database_manager",
                      config_name=config_name)
                raise ValueError("Country parameter is required")
            
            info(f"Loading GeoServer configuration for {config_name} from {country}", 
                 component="database_manager",
                 config_name=config_name,
                 country=country)
            
            # First, get the country data to obtain the country_id
            country_results = self.country_service.get_by_name(country)
            if not country_results:
                error(f"Country not found: {country}",
                      component="database_manager",
                      country=country)
                raise ValueError(f"Country '{country}' not found in database")
            
            country_data = country_results[0]
            all_configs = self.data_source_service.get_by_country(country_id=country_data.id)
            
            if not all_configs:
                warning(f"No configurations found for country: {country}",
                       component="database_manager",
                       country=country)
                return None
            
            # Filter by name within the country configurations
            db_config = next((config for config in all_configs if config.name == config_name), None)
            
            if not db_config or not db_config.content:
                warning("GeoServer configuration not found", 
                       component="database_manager",
                       config_name=config_name,
                       country=country)
                return None
            
            # Parse JSON content
            config_content = json.loads(db_config.content)
            
            info("GeoServer configuration loaded successfully",
                 component="database_manager",
                 config_name=config_name,
                 country=country)
            
            return config_content
            
        except json.JSONDecodeError as e:
            error("Invalid JSON in GeoServer configuration",
                  component="database_manager",
                  config_name=config_name,
                  country=country,
                  error=str(e))
            return None
        except Exception as e:
            error("Failed to load GeoServer configuration",
                  component="database_manager",
                  config_name=config_name,
                  country=country,
                  error=str(e))
            return None
    
    def get_measure_id_by_short_name(self, short_name: str) -> Optional[int]:
        """
        Get measure ID by short name.
        
        Args:
            short_name: Short name of the climate measure (e.g., 'tmax', 'tmin', 'prec')
            
        Returns:
            Measure ID if found, None otherwise
        """
        try:
            measures = self.climate_measure_service.get_by_short_name(short_name, enabled=True)
            if measures and len(measures) > 0:
                return measures[0].id
            else:
                warning(f"Climate measure not found for short_name: {short_name}", 
                       component="database_manager")
                return None
        except Exception as e:
            error(f"Failed to get measure ID for short_name: {short_name}", 
                  component="database_manager",
                  error=str(e))
            return None
    
    def get_climate_measure_mapping(self) -> Dict[str, int]:
        """
        Get mapping of climate variable names to measure IDs.
        
        Returns:
            Dictionary mapping variable names to measure IDs
        """
        try:
            # Common climate variable mappings
            variable_mappings = {
                'tmax': 'tmax',
                'tmin': 'tmin', 
                'prec': 'prec',
                'sol_rad': 'sol_rad',
                'temperature_max': 'tmax',
                'temperature_min': 'tmin',
                'precipitation': 'prec',
                'solar_radiation': 'sol_rad'
            }
            
            measure_mapping = {}
            for var_name, short_name in variable_mappings.items():
                measure_id = self.get_measure_id_by_short_name(short_name)
                if measure_id:
                    measure_mapping[var_name] = measure_id
                    
            info(f"Climate measure mapping loaded", 
                 component="database_manager",
                 mappings_count=len(measure_mapping))
                 
            return measure_mapping
            
        except Exception as e:
            error("Failed to get climate measure mapping", 
                  component="database_manager",
                  error=str(e))
            return {}
    
    def save_extracted_data(self, extracted_data, country: str) -> bool:
        """
        Save extracted data from GeoServer to database.
        
        Args:
            extracted_data: DataFrame or data structure from GeoServer extraction
            country: Country name for context
            
        Returns:
            True if successful, False otherwise
        """
        try:
            info(f"Saving extracted data to database for {country} total records: {len(extracted_data) if hasattr(extracted_data, '__len__') else 'unknown'}", 
                 component="database_manager",
                 country=country,
                 total_records=len(extracted_data) if hasattr(extracted_data, '__len__') else 'unknown')
            
            # Get measure mapping
            measure_mapping = self.get_climate_measure_mapping()
            if not measure_mapping:
                error("No climate measure mappings found", component="database_manager")
                return False
            
            # Check if extracted_data is a pandas DataFrame
            if hasattr(extracted_data, 'iterrows'):
                # Handle DataFrame format
                saved_count = 0
                error_count = 0
                
                for index, row in extracted_data.iterrows():
                    try:
                        location_id = row.get('location_id')
                        date = row.get('date')
                        
                        if not location_id or not date:
                            warning(f"Missing location_id or date in row {index}", 
                                   component="database_manager")
                            error_count += 1
                            continue
                        
                        # Process each climate variable in the row
                        climate_variables = {
                            'tmax': row.get('tmax'),
                            'tmin': row.get('tmin'),
                            'prec': row.get('prec'),
                            'sol_rad': row.get('sol_rad')
                        }
                        
                        for var_name, value in climate_variables.items():
                            if value is not None and var_name in measure_mapping:
                                try:
                                    # Create ClimateHistoricalDailyCreate schema instance
                                    historical_data = ClimateHistoricalDailyCreate(
                                        location_id=location_id,
                                        measure_id=measure_mapping[var_name],
                                        date=date,
                                        value=float(value)
                                    )
                                    
                                    # Save using the historical data service
                                    result = self.historical_data_service.create(historical_data)
                                    if result:
                                        saved_count += 1
                                    else:
                                        error_count += 1
                                        
                                except Exception as var_error:
                                    error_count += 1
                                    warning(f"Failed to save {var_name} for row {index}", 
                                           component="database_manager",
                                           variable=var_name,
                                           value=value,
                                           error=str(var_error))
                            
                    except Exception as row_error:
                        error_count += 1
                        warning(f"Failed to process row {index}", 
                               component="database_manager",
                               error=str(row_error))
                
                info(f"Data saving completed", 
                     component="database_manager",
                     saved_records=saved_count,
                     failed_records=error_count)
                
                return error_count == 0
            
            else:
                # Handle other data formats (list, dict, etc.)
                warning("Extracted data format not recognized as DataFrame", 
                       component="database_manager",
                       data_type=type(extracted_data).__name__)
                return False
                
        except Exception as e:
            error("Failed to save extracted data", 
                  component="database_manager",
                  country=country,
                  error=str(e))
            return False
