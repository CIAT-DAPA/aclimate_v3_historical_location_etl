import json
from typing import Any, Dict, List, Optional

import pandas as pd

from .tools.logging_manager import error, info, warning
from .tools.tools import DownloadProgressBar

# ORM imports - Required for database operations
try:
    from aclimate_v3_orm.schemas import (
        ClimateHistoricalDailyCreate,
        ClimateHistoricalMonthlyCreate,
        LocationRead,
    )
    from aclimate_v3_orm.services import (
        ClimateHistoricalDailyService,
        ClimateHistoricalMonthlyService,
        MngClimateMeasureService,
        MngCountryService,
        MngDataSourceService,
        MngLocationService,
    )
except ImportError as e:
    error(
        "ORM (aclimate_v3_orm) is required for database operations",
        component="database_manager",
        error=str(e),
    )
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
        self.historical_monthly_service = ClimateHistoricalMonthlyService()

        info("Database manager initialized", component="database_manager")

    def get_locations_by_ids(
        self, location_ids: str, country: str
    ) -> List[LocationRead]:
        """Get locations by comma-separated IDs."""
        try:
            info(
                "Retrieving locations by IDs",
                component="database_manager",
                location_ids=location_ids,
                country=country,
            )

            ids = [int(id.strip()) for id in location_ids.split(",")]
            locations = []

            for location_id in ids:
                location = self.location_service.get_by_id(location_id)
                if location:
                    locations.append(location)
                    info(
                        f"Found location: {location.name}",
                        component="database_manager",
                        location_id=location_id,
                        location_name=location.name,
                    )
                else:
                    warning(
                        f"Location not found: {location_id}",
                        component="database_manager",
                        location_id=location_id,
                    )

            if not locations:
                raise ValueError(f"No valid locations found for IDs: {location_ids}")

            info(
                f"Retrieved {len(locations)} locations",
                component="database_manager",
                location_count=len(locations),
            )
            return locations

        except ValueError as e:
            error(
                "Invalid location ID format", component="database_manager", error=str(e)
            )
            raise ValueError(
                f"Invalid location ID format. Use comma-separated integers. "
                f"Error: {str(e)}"
            )
        except Exception as e:
            error(
                "Failed to retrieve locations",
                component="database_manager",
                error=str(e),
            )
            raise Exception(f"Failed to retrieve locations: {str(e)}")

    def get_all_locations(self, country: str) -> List[LocationRead]:
        """Get all locations from database for the country."""
        try:
            info(
                "Retrieving all locations from database",
                component="database_manager",
                country=country,
            )
            locations = self.location_service.get_by_country_name(country)

            info(
                f"Retrieved {len(locations)} locations from database",
                component="database_manager",
                location_count=len(locations),
            )

            if not locations:
                warning(
                    f"No locations found for country: {country}",
                    component="database_manager",
                    country=country,
                )

            return locations

        except Exception as e:
            error(
                "Failed to retrieve all locations",
                component="database_manager",
                error=str(e),
            )
            raise Exception(f"Failed to retrieve all locations: {str(e)}")

    def validate_location_exists(self, location_id: int) -> bool:
        """Validate that a location exists in the database."""
        try:
            location = self.location_service.get_by_id(location_id)
            return location is not None
        except Exception as e:
            error(
                "Failed to validate location existence",
                component="database_manager",
                location_id=location_id,
                error=str(e),
            )
            return False

    def get_location_info(self, location_id: int) -> Optional[LocationRead]:
        """Get location information using ORM schema directly."""
        try:
            # Return the location schema directly - no need for additional
            # object creation. The LocationRead schema already contains all
            # necessary information and is properly typed and formatted
            location = self.location_service.get_by_id(location_id)

            if location:
                info(
                    "Location information retrieved",
                    component="database_manager",
                    location_id=location_id,
                    location_name=location.name,
                )
            else:
                warning(
                    "Location not found",
                    component="database_manager",
                    location_id=location_id,
                )

            return location

        except Exception as e:
            error(
                "Failed to get location info",
                component="database_manager",
                location_id=location_id,
                error=str(e),
            )
            return None

    def get_geoserver_config(
        self, config_name: str, country: str
    ) -> Optional[Dict[str, Any]]:
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
                error(
                    "Country parameter is required for GeoServer configuration lookup",
                    component="database_manager",
                    config_name=config_name,
                )
                raise ValueError("Country parameter is required")

            info(
                f"Loading GeoServer configuration for {config_name} from {country}",
                component="database_manager",
                config_name=config_name,
                country=country,
            )

            # First, get the country data to obtain the country_id
            country_results = self.country_service.get_by_name(country)
            if not country_results:
                error(
                    f"Country not found: {country}",
                    component="database_manager",
                    country=country,
                )
                raise ValueError(f"Country '{country}' not found in database")

            country_data = country_results[0]
            all_configs = self.data_source_service.get_by_country(
                country_id=country_data.id
            )

            if not all_configs:
                warning(
                    f"No configurations found for country: {country}",
                    component="database_manager",
                    country=country,
                )
                return None

            # Filter by name within the country configurations
            db_config = next(
                (config for config in all_configs if config.name == config_name), None
            )

            if not db_config or not db_config.content:
                warning(
                    "GeoServer configuration not found",
                    component="database_manager",
                    config_name=config_name,
                    country=country,
                )
                return None

            # Parse JSON content
            config_content = json.loads(db_config.content)

            info(
                "GeoServer configuration loaded successfully",
                component="database_manager",
                config_name=config_name,
                country=country,
            )

            return config_content

        except json.JSONDecodeError as e:
            error(
                "Invalid JSON in GeoServer configuration",
                component="database_manager",
                config_name=config_name,
                country=country,
                error=str(e),
            )
            return None
        except Exception as e:
            error(
                "Failed to load GeoServer configuration",
                component="database_manager",
                config_name=config_name,
                country=country,
                error=str(e),
            )
            return None

    def _get_measure_mapping(
        self, country: str, geoserver_config: Dict[str, Any] = None
    ) -> Dict[str, int]:
        """
        Get measure mapping from geoserver config or fallback to default mapping.

        Args:
            country: Country name
            geoserver_config: GeoServer configuration dictionary (optional)

        Returns:
            Dictionary mapping variable names to measure IDs
        """
        if geoserver_config:
            variable_mapping = self.get_variable_mapping_from_geoserver_config(
                country, geoserver_config
            )
            if variable_mapping:
                measure_mapping = {}
                for config_var, db_measure in variable_mapping.items():
                    measure_id = self.get_measure_id_by_short_name(db_measure)
                    if measure_id:
                        measure_mapping[config_var] = measure_id
                return measure_mapping
            else:
                warning(
                    "No variable mapping found from config, "
                    "falling back to default mapping",
                    component="database_manager",
                )

        # Fallback to default mapping
        return self.get_climate_measure_mapping()

    def get_measure_id_by_short_name(self, short_name: str) -> Optional[int]:
        """
        Get measure ID by short name.

        Args:
            short_name: Short name of the climate measure (e.g., 'tmax', 'tmin', 'prec')

        Returns:
            Measure ID if found, None otherwise
        """
        try:
            measures = self.climate_measure_service.get_by_short_name(
                short_name, enabled=True
            )

            if measures and len(measures) > 0:
                measure_id = measures[0].id
                return measure_id
            else:
                warning(
                    f"Climate measure not found for short_name: {short_name}",
                    component="database_manager",
                    short_name=short_name,
                )
                return None
        except Exception as e:
            error(
                f"Failed to get measure ID for short_name: {short_name}",
                component="database_manager",
                short_name=short_name,
                error=str(e),
            )
            return None

    def _save_data_with_progress(
        self,
        data: pd.DataFrame,
        country: str,
        data_type: str,
        service_create_func,
        schema_class,
        measure_mapping: Dict[str, int],
    ) -> bool:
        """
        Generic method to save climate data with progress bar.

        Args:
            data: DataFrame with climate data
            country: Country name for context
            data_type: Type of data being saved (for logging)
            service_create_func: Function to create data in database
            schema_class: Schema class for data creation
            measure_mapping: Mapping of variable names to measure IDs

        Returns:
            True if successful, False otherwise
        """
        if data.empty:
            warning(f"No {data_type} data to save", component="database_manager")
            return True

        saved_count = 0
        error_count = 0
        total_rows = len(data)

        with DownloadProgressBar(
            total=total_rows, desc=f"Saving {data_type} data for {country}", unit="rows"
        ) as pbar:

            for index, row in data.iterrows():
                try:
                    row_saved, row_errors = self._process_climate_variables(
                        row,
                        set(data.columns),
                        measure_mapping,
                        service_create_func,
                        schema_class,
                    )
                    saved_count += row_saved
                    error_count += row_errors

                except Exception as row_error:
                    error_count += 1
                    warning(
                        f"Failed to process row {index}",
                        component="database_manager",
                        error=str(row_error),
                    )
                finally:
                    pbar.update(1)

        info(
            f"{data_type.title()} data processing completed",
            component="database_manager",
            saved_records=saved_count,
            failed_records=error_count,
        )

        return error_count == 0

    def _process_climate_variables(
        self,
        row: pd.Series,
        data_columns: set,
        measure_mapping: Dict[str, int],
        service_create_func,
        schema_class,
        date_field: str = "date",
    ) -> tuple[int, int]:
        """
        Process climate variables for a single row and save to database.

        Args:
            row: DataFrame row containing climate data
            data_columns: Set of all DataFrame columns
            measure_mapping: Mapping of variable names to measure IDs
            service_create_func: Function to create data in database
            schema_class: Schema class for data creation
            date_field: Name of the date field in the row

        Returns:
            Tuple of (saved_count, error_count)
        """
        saved_count = 0
        error_count = 0

        location_id = row.get("location_id")
        date_value = row.get(date_field)

        if not location_id or not date_value:
            return 0, 1

        # Process each climate variable in the row
        metadata_columns = {
            "location_id",
            "location_name",
            "latitude",
            "longitude",
            "date",
            "year",
            "month",
        }
        climate_variables = {
            col: row.get(col) for col in data_columns if col not in metadata_columns
        }

        for var_name, value in climate_variables.items():
            if value is not None and var_name in measure_mapping:
                try:
                    # Validate numerical values
                    numeric_value = float(value)
                    if str(numeric_value).lower() in ["nan", "inf", "-inf"]:
                        warning(
                            f"Invalid numeric value {numeric_value} for {var_name}",
                            component="database_manager",
                            variable=var_name,
                            value=numeric_value,
                        )
                        continue

                    # Create and save data object
                    data_obj = schema_class(
                        location_id=location_id,
                        measure_id=measure_mapping[var_name],
                        date=date_value,
                        value=numeric_value,
                    )
                    result = service_create_func(data_obj)

                    if result:
                        saved_count += 1
                    else:
                        error_count += 1

                except ValueError as val_error:
                    error_count += 1
                    warning(
                        f"Invalid value for {var_name}: {value}",
                        component="database_manager",
                        variable=var_name,
                        value=value,
                        error=str(val_error),
                    )
                except Exception as var_error:
                    error_count += 1
                    warning(
                        f"Failed to save {var_name}",
                        component="database_manager",
                        variable=var_name,
                        value=value,
                        error=str(var_error),
                    )

        return saved_count, error_count

    def get_climate_measure_mapping(self) -> Dict[str, int]:
        """
        Get mapping of climate variable names to measure IDs.

        Returns:
            Dictionary mapping variable names to measure IDs
        """
        try:
            # Common climate variable mappings
            variable_mappings = {
                "tmax": "tmax",
                "tmin": "tmin",
                "prec": "prec",
                "sol_rad": "sol_rad",
                "temperature_max": "tmax",
                "temperature_min": "tmin",
                "precipitation": "prec",
                "solar_radiation": "sol_rad",
            }

            measure_mapping = {}
            for var_name, short_name in variable_mappings.items():
                measure_id = self.get_measure_id_by_short_name(short_name)
                if measure_id:
                    measure_mapping[var_name] = measure_id
                else:
                    warning(
                        f"No measure ID found for {var_name} "
                        f"(short_name: {short_name})",
                        component="database_manager",
                        variable=var_name,
                        short_name=short_name,
                    )

            info(
                "Climate measure mapping loaded",
                component="database_manager",
                mappings_count=len(measure_mapping),
            )

            return measure_mapping

        except Exception as e:
            error(
                "Failed to get climate measure mapping",
                component="database_manager",
                error=str(e),
            )
            return {}

    def get_variable_mapping_from_geoserver_config(
        self, country: str, geoserver_config: Dict[str, Any]
    ) -> Dict[str, str]:
        """
        Get variable mapping from provided GeoServer configuration.
        Maps config variable names to database measure short names.

        Args:
            country: Country name
            geoserver_config: GeoServer configuration dictionary

        Returns:
            Dictionary mapping config variables to measure short names
        """
        try:
            # Extract variable mappings from config
            variable_mapping = {}

            # Get country-specific config
            country_config = geoserver_config.get(country.upper(), {})
            if not country_config:
                warning(
                    f"No country configuration found for {country}",
                    component="database_manager",
                )
                return {}

            # Map each variable in config to its database measure
            for config_var_name, layer_config in country_config.items():
                # Get the database measure name from layer config or use
                # fallback mapping
                db_measure = layer_config.get("measure")

                if not db_measure:
                    # Fallback mapping for common variables
                    fallback_mapping = {
                        "rad": "sol_rad",
                        "prec": "prec",
                        "tmax": "tmax",
                        "tmin": "tmin",
                        "precipitation": "prec",
                        "temperature_max": "tmax",
                        "temperature_min": "tmin",
                        "solar_radiation": "sol_rad",
                    }
                    db_measure = fallback_mapping.get(config_var_name, config_var_name)

                variable_mapping[config_var_name] = db_measure

                info(
                    f"Mapped config variable '{config_var_name}' "
                    f"to database measure '{db_measure}'",
                    component="database_manager",
                    config_var=config_var_name,
                    db_measure=db_measure,
                )

            return variable_mapping

        except Exception as e:
            error(
                "Failed to get variable mapping from geoserver config",
                component="database_manager",
                country=country,
                error=str(e),
            )
            return {}

    def save_extracted_data(
        self, extracted_data, country: str, geoserver_config: Dict[str, Any] = None
    ) -> bool:
        """
        Save extracted data from GeoServer to database.

        Args:
            extracted_data: DataFrame or data structure from GeoServer extraction
            country: Country name for context
            geoserver_config: GeoServer configuration dictionary (optional)

        Returns:
            True if successful, False otherwise
        """
        try:
            record_count = (
                len(extracted_data) if hasattr(extracted_data, "__len__") else "unknown"
            )
            info(
                f"Saving extracted data to database for {country} "
                f"total records: {record_count}",
                component="database_manager",
                country=country,
                total_records=record_count,
            )

            # Validate DataFrame format
            if not hasattr(extracted_data, "iterrows"):
                warning(
                    "Extracted data format not recognized as DataFrame",
                    component="database_manager",
                    data_type=type(extracted_data).__name__,
                )
                return False

            # Get measure mapping
            measure_mapping = self._get_measure_mapping(country, geoserver_config)
            if not measure_mapping:
                error("No climate measure mappings found", component="database_manager")
                return False

            # Save data using helper method
            return self._save_data_with_progress(
                extracted_data,
                country,
                "climate",
                self.historical_data_service.create,
                ClimateHistoricalDailyCreate,
                measure_mapping,
            )

        except Exception as e:
            error(
                "Failed to save extracted data",
                component="database_manager",
                country=country,
                error=str(e),
            )
            return False

    def save_monthly_data(
        self,
        monthly_data: pd.DataFrame,
        country: str,
        geoserver_config: Dict[str, Any] = None,
    ) -> bool:
        """
        Save monthly aggregated data to database.

        Args:
            monthly_data: DataFrame with monthly aggregated climate data
            country: Country name for context
            geoserver_config: GeoServer configuration dictionary (optional)

        Returns:
            True if successful, False otherwise
        """
        try:
            info(
                f"Saving monthly aggregated data to database for {country} "
                f"total records: {len(monthly_data)}",
                component="database_manager",
                country=country,
                total_records=len(monthly_data),
            )

            # Get measure mapping
            measure_mapping = self._get_measure_mapping(country, geoserver_config)
            if not measure_mapping:
                error(
                    "No climate measure mappings found for monthly data",
                    component="database_manager",
                )
                return False

            # Save data using helper method
            return self._save_data_with_progress(
                monthly_data,
                country,
                "monthly",
                ClimateHistoricalMonthlyService().create,
                ClimateHistoricalMonthlyCreate,
                measure_mapping,
            )

        except Exception as e:
            error(
                "Failed to save monthly data",
                component="database_manager",
                country=country,
                error=str(e),
            )
            return False
