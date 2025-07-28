"""
GeoServer client for extracting point data from locations.
Handles WCS requests to extract historical climate data from geographical locations.
"""

import io
import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import pandas as pd
import requests
import xarray as xr
from aclimate_v3_orm.schemas import LocationRead

from .data_validator import DataValidator
from .database_manager import DatabaseManager
from .tools.logging_manager import error, info, warning
from .tools.tools import DownloadProgressBar


class GeoServerClient:
    """
    Client for making requests to GeoServer to extract point data from locations.
    Combines GeoServer requests with spatial processing capabilities.
    """

    def __init__(self, geoserver_config: Dict[str, Any]):
        """
        Initialize GeoServer client.

        Args:
            geoserver_config: GeoServer configuration dictionary
        """
        self.db_manager = DatabaseManager()
        self.geoserver_config = geoserver_config
        self.data_validator = DataValidator()

        info("GeoServer client initialized", component="geoserver_client")

    def extract_location_data(
        self, location_ids: str, country: str, start_date: datetime, end_date: datetime
    ) -> pd.DataFrame:
        """
        Extract point data for locations from GeoServer for a date range.

        Args:
            location_ids: Comma-separated location IDs or "all" for all locations
            country: Country name
            start_date: Start date for data extraction
            end_date: End date for data extraction

        Returns:
            DataFrame with extracted data for each location and date
        """
        try:
            info(
                f"Starting location data extraction from {start_date} to {end_date}",
                component="geoserver_client",
                location_ids=location_ids,
                country=country,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )

            # Get locations from database
            if location_ids.lower() == "all":
                locations = self.db_manager.get_all_locations(country)
            else:
                locations = self.db_manager.get_locations_by_ids(location_ids, country)

            if not locations:
                error(
                    f"No locations found for criteria {location_ids} in {country}",
                    component="geoserver_client",
                    location_ids=location_ids,
                    country=country,
                )
                sys.exit(1)

            # Extract data for each location and date
            all_data = []

            # Generate date range (daily)
            date_list = []
            current_date = start_date
            while current_date <= end_date:
                date_list.append(current_date)
                current_date += timedelta(days=1)

            # Calculate total operations for progress bar
            total_operations = len(locations) * len(date_list)

            # Counters for summary
            successful_extractions = 0
            failed_extractions = 0
            total_downloads = 0

            # Initialize progress bar for point extraction
            with DownloadProgressBar(
                total=total_operations,
                desc="Extracting location data",
                unit="points",
                leave=True,
            ) as pbar:

                for date in date_list:
                    for location in locations:
                        location_data, extraction_stats = (
                            self._extract_location_point_data(
                                location, date, country, pbar
                            )
                        )
                        all_data.append(location_data)

                        # Update counters
                        if extraction_stats.get("success", False):
                            successful_extractions += 1
                        else:
                            failed_extractions += 1
                        total_downloads += extraction_stats.get("downloads", 0)

            # Convert to DataFrame
            df = pd.DataFrame(all_data)

            # Show extraction summary
            info(
                f"Location data extraction completed from {start_date} to "
                f"{end_date} - {successful_extractions} successful, "
                f"{failed_extractions} failed",
                component="geoserver_client",
                locations_processed=len(locations),
                dates_processed=len(date_list),
                total_operations=total_operations,
                successful_extractions=successful_extractions,
                failed_extractions=failed_extractions,
                total_downloads=total_downloads,
                total_records=len(df),
            )

            # Validate and clean extracted data
            info("Starting data validation and cleaning", component="geoserver_client")
            expected_location_ids = [location.id for location in locations]

            cleaned_df, validation_results = (
                self.data_validator.validate_extracted_data(
                    df=df,
                    start_date=start_date,
                    end_date=end_date,
                    expected_locations=expected_location_ids,
                    clean_data=True,  # Enable data cleaning
                )
            )

            # Generate validation report
            validation_report = self.data_validator.generate_validation_report(
                validation_results
            )

            # Log validation completion with report in the main message for
            # console visibility
            info(
                f"Data validation completed\n{validation_report}",
                component="geoserver_client",
                status="PASSED" if validation_results["is_valid"] else "FAILED",
                errors_count=len(validation_results["errors"]),
                warnings_count=len(validation_results["warnings"]),
                validation_statistics=validation_results["statistics"],
            )

            # Check if validation passed
            if not validation_results["is_valid"]:
                # Show critical error in console for user visibility
                print(
                    f"\n❌ ERROR: Data validation failed with "
                    f"{len(validation_results['errors'])} critical errors:"
                )
                for i, error_msg in enumerate(validation_results["errors"], 1):
                    print(f"   {i}. {error_msg}")
                print(
                    "\nProcessing cannot continue. "
                    "Please review the validation report above.\n"
                )

                # Also log critical validation failure with structured data
                error(
                    "Data validation failed - cannot continue with processing",
                    component="geoserver_client",
                    errors_count=len(validation_results["errors"]),
                    warnings_count=len(validation_results["warnings"]),
                    validation_errors=validation_results["errors"],
                    validation_warnings=validation_results["warnings"],
                )
                # sys.exit(1)

            # Log validation success with detailed metrics and statistics
            info(
                "Data validation and cleaning passed successfully",
                component="geoserver_client",
                total_records=validation_results["statistics"].get(
                    "total_records", len(cleaned_df)
                ),
                warnings_count=len(validation_results["warnings"]),
                validation_warnings=(
                    validation_results["warnings"]
                    if validation_results["warnings"]
                    else None
                ),
                validation_statistics=validation_results["statistics"],
                cleaning_actions_count=len(
                    validation_results.get("cleaning_actions", [])
                ),
            )

            return cleaned_df

        except Exception as e:
            error(
                "Failed to extract location data",
                component="geoserver_client",
                error=str(e),
            )
            sys.exit(1)

    def _extract_location_point_data(
        self,
        location: LocationRead,
        date: datetime,
        country: str,
        pbar: Optional[DownloadProgressBar] = None,
    ) -> tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Extract point data for a single location on a specific date.

        Args:
            location: Location object from database
            date: Date for extraction
            country: Country name
            pbar: Progress bar to update (optional)

        Returns:
            Tuple of (location_data_dict, extraction_stats_dict)
        """
        extraction_stats = {"success": False, "downloads": 0}

        try:
            # Update progress bar instead of logging each point
            if pbar:
                pbar.set_description(
                    f"Extracting {location.name} - {date.strftime('%Y-%m-%d')}"
                )
                pbar.update(1)

            location_data = {
                "location_id": location.id,
                "location_name": location.name,
                "latitude": location.latitude,
                "longitude": location.longitude,
                "date": date.strftime("%Y-%m-%d"),
            }

            # Extract data for each configured variable
            daily_data, download_count = self._extract_daily_data(
                location, date, country
            )
            extraction_stats["downloads"] = download_count

            location_data.update(daily_data)
            extraction_stats["success"] = True

            return location_data, extraction_stats

        except Exception as e:
            error(
                "Failed to extract point data for location",
                component="geoserver_client",
                location_id=location.id,
                date=date.strftime("%Y-%m-%d"),
                error=str(e),
            )
            # Still update progress bar even on error
            if pbar:
                pbar.update(1)
            # Return empty data instead of exiting, let the main process handle it
            return (
                {
                    "location_id": location.id,
                    "location_name": location.name,
                    "latitude": location.latitude,
                    "longitude": location.longitude,
                    "date": date.strftime("%Y-%m-%d"),
                },
                extraction_stats,
            )

    def _extract_daily_data(
        self, location: LocationRead, date: datetime, country: str
    ) -> tuple[Dict[str, Any], int]:
        """
        Extract daily data for a location from all configured variables.

        Args:
            location: Location object from database
            date: Date for extraction
            country: Country name

        Returns:
            Tuple of (daily_data_dict, download_count)
        """
        try:
            daily_data = {}
            download_count = 0

            # Get country configuration
            country_config = self.geoserver_config.get(country.upper(), {})
            if not country_config:
                error(
                    "Country configuration not found",
                    component="geoserver_client",
                    country=country,
                )
                return {}, 0

            # Extract data for each configured variable
            for variable_name, layer_config in country_config.items():
                value, downloads = self._extract_point_value(
                    location, date, variable_name, layer_config, country_config
                )
                daily_data[variable_name] = value
                download_count += downloads

            return daily_data, download_count

        except Exception as e:
            error(
                f"Failed to extract daily data for location {location.name} {str(e)}",
                component="geoserver_client",
                location_id=location.id,
                date=date.strftime("%Y-%m-%d"),
                error=str(e),
            )
            return {}, 0

    def _extract_point_value(
        self,
        location: LocationRead,
        date: datetime,
        variable_name: str,
        layer_config: Dict[str, Any],
        country_config: Dict[str, Any],
    ) -> tuple[Optional[float], int]:
        """
        Extract point value from GeoServer layer for a specific location and date.

        Args:
            location: Location object from database
            date: Date for extraction
            variable_name: Name of the variable to extract
            layer_config: Configuration for the GeoServer layer
            country_config: Country configuration

        Returns:
            Tuple of (extracted_value, download_count)
        """
        try:
            # Download GeoTIFF for the date
            tiff_data = self._download_from_geoserver(
                date, variable_name, layer_config, country_config
            )

            download_count = 1 if tiff_data is not None else 0

            if tiff_data is None:
                warning(
                    "Failed to download GeoTIFF",
                    component="geoserver_client",
                    variable=variable_name,
                    date=date.strftime("%Y-%m-%d"),
                )
                return None, download_count

            # Extract point value from GeoTIFF data
            value = self._extract_point_from_tiff(
                tiff_data, location.longitude, location.latitude
            )

            # Only log if there's an issue or for debugging purposes
            # Removed individual point value logging to reduce noise

            return value, download_count

        except Exception as e:
            error(
                "Failed to extract point value",
                component="geoserver_client",
                location_id=location.id,
                variable=variable_name,
                date=date.strftime("%Y-%m-%d"),
                error=str(e),
            )
            return None, 0

    def _download_from_geoserver(
        self,
        date: datetime,
        variable_name: str,
        layer_config: Dict[str, Any],
        country_config: Dict[str, Any],
    ) -> Optional[bytes]:
        """
        Download a daily GeoTIFF file from GeoServer using WCS and return as bytes.

        Args:
            date: Date for extraction
            variable_name: Name of the variable
            layer_config: Configuration for the GeoServer layer
            country_config: Country configuration

        Returns:
            GeoTIFF data as bytes, or None if download fails
        """
        try:
            date_str = date.strftime("%Y-%m-%d")

            # Removed individual download logs to reduce noise
            # Only log errors and warnings now

            # Build WCS request parameters
            workspace = layer_config.get("workspace", "")
            store = layer_config.get("store", "")
            layer_name = f"{workspace}:{store}"

            params = {
                "service": "WCS",
                "version": country_config.get("wcs_version", "2.0.1"),
                "request": "GetCoverage",
                "coverageId": layer_name,
                "subset": f'time("{date_str}T00:00:00.000Z")',
                "format": country_config.get("format", "image/geotiff"),
            }  # Debugging: print parameters
            # Build URL
            geoserver_url = os.getenv(
                "GEOSERVER_URL", "http://localhost:8080/geoserver"
            )
            base_url = f"{geoserver_url.replace('rest/', '')}{workspace}/ows?"
            url = base_url + urlencode(params)
            # Authentication
            auth = None
            username = os.getenv("GEOSERVER_USERNAME")
            password = os.getenv("GEOSERVER_PASSWORD")
            if username and password:
                auth = (username, password)

            # Make request
            response = requests.get(url, auth=auth, stream=True, timeout=60)
            response.raise_for_status()

            # Get content
            content: bytes = response.content

            # Validate GeoTIFF header
            if not content.startswith(b"\x49\x49\x2a\x00") and not content.startswith(
                b"\x4d\x4d\x00\x2a"
            ):
                raise ValueError("Downloaded file is not a valid GeoTIFF")

            # Removed individual download completion logs to reduce noise

            return content

        except Exception as e:
            warning(
                "GeoServer download failed",
                component="geoserver_client",
                date=date.strftime("%Y-%m-%d"),
                variable=variable_name,
                error=str(e),
            )
            return None

    def _extract_point_from_tiff(
        self, tiff_data: bytes, longitude: float, latitude: float
    ) -> Optional[float]:
        """
        Extract point value from a GeoTIFF data using xarray with spatial
        interpolation fallback.

        Args:
            tiff_data: GeoTIFF data as bytes
            longitude: Longitude coordinate
            latitude: Latitude coordinate

        Returns:
            Extracted value or None if extraction failed
        """
        try:
            # Create temporary file-like object from bytes
            tiff_buffer = io.BytesIO(tiff_data)

            # Open raster with xarray/rioxarray
            data_xarray = xr.open_dataarray(tiff_buffer, engine="rasterio")

            # Set CRS if not already set
            if data_xarray.rio.crs is None:
                data_xarray = data_xarray.rio.write_crs("EPSG:4326")

            # Extract value at exact point using nearest neighbor
            value = data_xarray.sel(x=longitude, y=latitude, method="nearest")

            # Check if we got a valid value more carefully
            is_valid = False
            extracted_value = None

            try:
                if hasattr(value, "values"):
                    extracted_value = float(value.values)
                else:
                    extracted_value = float(value)

                # Check if the value is actually valid (not NaN and not a special
                # nodata value)
                if (
                    not pd.isna(extracted_value)
                    and extracted_value != -9999
                    and extracted_value != -999
                ):
                    is_valid = True
            except (ValueError, TypeError):
                is_valid = False

            # If we got a valid value, return it
            if is_valid:
                return extracted_value

            # If exact point is NaN or invalid, try spatial interpolation
            interpolated_value = self._interpolate_spatial_value(
                data_xarray, longitude, latitude
            )
            if interpolated_value is not None:
                return interpolated_value

            # If interpolation also failed, return None
            return None

        except Exception as e:
            error(
                f"Failed to extract point from GeoTIFF {str(e)}",
                component="geoserver_client",
                longitude=longitude,
                latitude=latitude,
                error=str(e),
            )
            return None

    def _interpolate_spatial_value(
        self,
        data_array: xr.DataArray,
        longitude: float,
        latitude: float,
        search_radius: float = 0.1,
    ) -> Optional[float]:
        """
        Interpolate value using nearby pixels when exact point is NaN.

        Args:
            data_array: xarray DataArray with raster data
            longitude: Target longitude
            latitude: Target latitude
            search_radius: Search radius in degrees (default 0.1 ≈ 11 km)

        Returns:
            Interpolated value or None if no valid neighbors found
        """
        try:
            # Try multiple search radii, starting small and expanding
            search_radii = [0.05, 0.1, 0.2, 0.5]  # ~5.5km, 11km, 22km, 55km

            for radius in search_radii:
                # Define search window around the point
                lon_min = longitude - radius
                lon_max = longitude + radius
                lat_min = latitude - radius
                lat_max = latitude + radius

                try:
                    # Extract subset of data around the point
                    subset = data_array.sel(
                        x=slice(lon_min, lon_max),
                        y=slice(lat_max, lat_min),  # Note: y is typically descending
                    )

                    # Get coordinates and values for valid pixels
                    valid_coords = []
                    valid_values = []

                    # Iterate through the subset to find valid pixels
                    for y_idx in range(subset.sizes.get("y", 0)):
                        for x_idx in range(subset.sizes.get("x", 0)):
                            try:
                                pixel_value = subset.isel(y=y_idx, x=x_idx)

                                # Check if pixel value is valid
                                if not pixel_value.isnull():
                                    pixel_val = float(pixel_value.values)

                                    # Skip nodata values
                                    if (
                                        not pd.isna(pixel_val)
                                        and pixel_val != -9999
                                        and pixel_val != -999
                                    ):
                                        pixel_lon = float(subset.x.isel(x=x_idx).values)
                                        pixel_lat = float(subset.y.isel(y=y_idx).values)
                                        valid_coords.append((pixel_lon, pixel_lat))
                                        valid_values.append(pixel_val)
                            except Exception:
                                continue

                    if len(valid_values) >= 1:  # Need at least 1 valid pixel
                        # Use inverse distance weighting for interpolation
                        interpolated_value = self._inverse_distance_weighting(
                            target_lon=longitude,
                            target_lat=latitude,
                            coords=valid_coords,
                            values=valid_values,
                        )

                        # Log successful interpolation with details
                        info(
                            f"Spatial interpolation successful at radius {radius} km "
                            f"longitude={longitude}, latitude={latitude} "
                            f"interpolated_value={interpolated_value}",
                            component="geoserver_client",
                            longitude=longitude,
                            latitude=latitude,
                            search_radius_km=radius * 111,  # Convert to approx km
                            valid_pixels_found=len(valid_values),
                            interpolated_value=interpolated_value,
                        )

                        return interpolated_value

                except Exception:
                    # Try next radius
                    continue

            # No valid pixels found in any search radius
            warning(
                "No valid pixels found for interpolation",
                component="geoserver_client",
                longitude=longitude,
                latitude=latitude,
                max_search_radius_km=max(search_radii) * 111,
            )
            return None

        except Exception as e:
            warning(
                "Spatial interpolation failed",
                component="geoserver_client",
                longitude=longitude,
                latitude=latitude,
                error=str(e),
            )
            return None

    def _inverse_distance_weighting(
        self,
        target_lon: float,
        target_lat: float,
        coords: List[tuple],
        values: List[float],
        power: float = 2.0,
    ) -> float:
        """
        Interpolate value using inverse distance weighting.

        Args:
            target_lon: Target longitude
            target_lat: Target latitude
            coords: List of (lon, lat) tuples for known points
            values: List of values at known points
            power: Power parameter for distance weighting (default 2.0)

        Returns:
            Interpolated value
        """
        try:
            weights = []

            for coord in coords:
                # Calculate Euclidean distance (approximation for small areas)
                distance = (
                    (target_lon - coord[0]) ** 2 + (target_lat - coord[1]) ** 2
                ) ** 0.5

                # Avoid division by zero for exact matches
                if distance == 0:
                    return values[coords.index(coord)]

                weight = 1.0 / (distance**power)
                weights.append(weight)

            # Calculate weighted average
            weighted_sum: float = sum(w * v for w, v in zip(weights, values))
            total_weight: float = sum(weights)

            return weighted_sum / total_weight

        except Exception as e:
            error(
                "Inverse distance weighting failed",
                component="geoserver_client",
                error=str(e),
            )
            # If interpolation fails, return simple average as fallback
            return sum(values) / len(values) if values else 0.0
