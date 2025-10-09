"""
CSV client for importing climate data from CSV files.
Handles CSV file reading and processing to extract historical climate data.
Supports multiple CSV files with format: (variable)_daily_data.csv
"""

import glob
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

from ..tools.logging_manager import error, info, warning
from .data_validator import DataValidator
from .database_manager import DatabaseManager


class CSVClient:
    """
    Client for importing climate data from CSV files.
    Processes CSV files and returns data in the same format as GeoServerClient.
    """

    def __init__(self) -> None:
        """Initialize CSV client."""
        self.db_manager = DatabaseManager()
        self.data_validator = DataValidator()
        info("CSV client initialized", component="csv_client")

    def extract_location_data(
        self,
        location_ids: str,
        country: str,
        start_date: datetime,
        end_date: datetime,
        csv_path: str,
    ) -> pd.DataFrame:
        """
        Extract climate data from CSV files for specified locations and date range.
        Searches for files with pattern: (variable)_daily_data.csv in the given path.

        Args:
            location_ids: Comma-separated location IDs or "all" for all locations in CSV
            country: Country name (for validation against database)
            start_date: Start date for data extraction
            end_date: End date for data extraction
            csv_path: Path to directory containing CSV files or single CSV file

        Returns:
            DataFrame with extracted data in the same format as GeoServerClient
        """
        try:
            info(
                f"Starting CSV data extraction from {start_date} to {end_date}",
                component="csv_client",
                location_ids=location_ids,
                country=country,
                csv_path=csv_path,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
            )

            # Find CSV files
            csv_files = self._find_csv_files(csv_path)
            if not csv_files:
                error(f"No CSV files found in path: {csv_path}", component="csv_client")
                raise FileNotFoundError(f"No CSV files found in path: {csv_path}")

            info(
                f"Found {len(csv_files)} CSV file(s) to process",
                component="csv_client",
                files=[f.name for f in csv_files],
            )

            # Read and combine all CSV files
            all_data = []
            for csv_file in csv_files:
                variable_name = self._extract_variable_name(csv_file)
                info(
                    f"Reading CSV file: {csv_file.name} (variable: {variable_name})",
                    component="csv_client",
                )

                df = self._read_csv_file(csv_file, variable_name)
                if df is not None and not df.empty:
                    all_data.append(df)

            if not all_data:
                error("No data could be read from CSV files", component="csv_client")
                raise ValueError("No data could be read from CSV files")

            # Combine all dataframes
            info("Combining data from all CSV files", component="csv_client")
            combined_df = self._combine_csv_data(all_data)

            info(
                f"Combined CSV data: {len(combined_df)} rows",
                component="csv_client",
                columns=list(combined_df.columns),
            )

            # Check if location identifier exists (either 'id' or 'ext_id')
            has_location_id = "id" in combined_df.columns
            has_ext_id = "ext_id" in combined_df.columns

            if not has_location_id and not has_ext_id:
                error(
                    "CSV must contain either 'id' or 'ext_id' column",
                    component="csv_client",
                )
                raise ValueError("CSV must contain either 'id' or 'ext_id' column")

            # Filter by date range
            combined_df = combined_df[
                (combined_df["date"] >= start_date) & (combined_df["date"] <= end_date)
            ]
            info(
                f"Filtered data by date range: {len(combined_df)} rows remaining",
                component="csv_client",
            )

            if combined_df.empty:
                warning(
                    f"No data found in CSV for date range {start_date} to {end_date}",
                    component="csv_client",
                )
                return pd.DataFrame()

            # Process locations based on selection mode
            if location_ids.lower() == "all":
                # Get all locations from CSV
                processed_df = self._process_all_csv_locations(
                    combined_df, country, has_ext_id
                )
            else:
                # Get specific locations
                processed_df = self._process_specific_locations(
                    combined_df, location_ids, country, has_ext_id
                )

            if processed_df.empty:
                warning(
                    "No valid data found after location processing",
                    component="csv_client",
                )
                return pd.DataFrame()

            info(
                f"CSV data extraction completed with {len(processed_df)} records",
                component="csv_client",
                unique_locations=(
                    processed_df["location_id"].nunique()
                    if "location_id" in processed_df.columns
                    else 0
                ),
                date_range=(
                    f"{processed_df['date'].min()} to {processed_df['date'].max()}"
                    if "date" in processed_df.columns
                    else "N/A"
                ),
            )

            # Validate and clean extracted data
            info("Starting data validation and cleaning", component="csv_client")
            expected_location_ids = processed_df["location_id"].unique().tolist()

            cleaned_df, validation_results = (
                self.data_validator.validate_extracted_data(
                    df=processed_df,
                    start_date=start_date,
                    end_date=end_date,
                    expected_locations=expected_location_ids,
                    clean_data=True,
                )
            )

            # Generate validation report
            validation_report = self.data_validator.generate_validation_report(
                validation_results
            )

            # Log validation completion
            info(
                f"Data validation completed\n{validation_report}",
                component="csv_client",
                status="PASSED" if validation_results["is_valid"] else "FAILED",
                errors_count=len(validation_results["errors"]),
                warnings_count=len(validation_results["warnings"]),
                validation_statistics=validation_results["statistics"],
            )

            # Check if validation passed
            if not validation_results["is_valid"]:
                error_msg = (
                    f"Data validation failed for CSV data:\n"
                    f"Errors: {validation_results['errors']}\n"
                    f"Warnings: {validation_results['warnings']}"
                )
                error(error_msg, component="csv_client")
                raise ValueError("Data validation failed")

            return cleaned_df

        except Exception as e:
            error(
                f"Error extracting data from CSV: {str(e)}",
                component="csv_client",
                error=str(e),
            )
            raise

    def _find_csv_files(self, csv_path: str) -> List[Path]:
        """
        Find CSV files matching the pattern (variable)_daily_data.csv

        Args:
            csv_path: Path to directory or file

        Returns:
            List of Path objects for CSV files to process
        """
        path = Path(csv_path)

        if path.is_file():
            # Single file provided
            return [path]
        elif path.is_dir():
            # Directory provided, search for *_daily_data.csv files
            pattern = str(path / "*_daily_data.csv")
            csv_files = [Path(f) for f in glob.glob(pattern)]

            if not csv_files:
                # Fallback: search for any .csv files
                pattern = str(path / "*.csv")
                csv_files = [Path(f) for f in glob.glob(pattern)]

            return csv_files
        else:
            return []

    def _extract_variable_name(self, csv_file: Path) -> str:
        """
        Extract variable name from CSV filename.
        Expected format: (variable)_daily_data.csv

        Args:
            csv_file: Path to CSV file

        Returns:
            Variable name extracted from filename
        """
        filename = csv_file.stem  # Get filename without extension

        # Try to extract variable name from pattern: variable_daily_data
        if filename.endswith("_daily_data"):
            variable_name = filename.replace("_daily_data", "")
            return variable_name

        # Fallback: use entire filename as variable name
        return filename

    def _read_csv_file(
        self, csv_file: Path, variable_name: str
    ) -> Optional[pd.DataFrame]:
        """
        Read a single CSV file with the expected format.
        Expected columns: ext_id (or id), day, month, year, value

        Args:
            csv_file: Path to CSV file
            variable_name: Name of the climate variable

        Returns:
            DataFrame with columns: location_id/ext_id, date, variable_name
        """
        try:
            # Read CSV with pyarrow engine
            df = pd.read_csv(csv_file, engine="pyarrow")

            info(
                f"Read {len(df)} rows from {csv_file.name}",
                component="csv_client",
                columns=list(df.columns),
            )

            # Validate required columns
            required_cols = ["day", "month", "year", "value"]
            has_id = "id" in df.columns
            has_ext_id = "ext_id" in df.columns

            if not (has_id or has_ext_id):
                error(
                    f"CSV {csv_file.name} must contain either 'id' or 'ext_id' column",
                    component="csv_client",
                )
                return None

            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                error(
                    f"CSV {csv_file.name} missing required columns: {missing_cols}",
                    component="csv_client",
                )
                return None

            # Create date column from day, month, year
            df["date"] = pd.to_datetime(
                df[["year", "month", "day"]].rename(
                    columns={"year": "year", "month": "month", "day": "day"}
                )
            )

            # Rename value column to variable name
            df = df.rename(columns={"value": variable_name})

            # Keep only necessary columns
            location_col = "id" if has_id else "ext_id"
            result_df = df[[location_col, "date", variable_name]].copy()

            return result_df

        except Exception as e:
            error(
                f"Error reading CSV file {csv_file.name}: {str(e)}",
                component="csv_client",
                error=str(e),
            )
            return None

    def _combine_csv_data(self, dataframes: List[pd.DataFrame]) -> pd.DataFrame:
        """
        Combine multiple dataframes from different CSV files.
        Each dataframe has: location_id/ext_id, date, variable_name

        Args:
            dataframes: List of dataframes to combine

        Returns:
            Combined dataframe with all variables
        """
        if not dataframes:
            return pd.DataFrame()

        if len(dataframes) == 1:
            return dataframes[0]

        # Determine which ID column to use
        first_df = dataframes[0]
        id_col = "id" if "id" in first_df.columns else "ext_id"

        # Merge all dataframes on location_id and date
        combined = dataframes[0]
        for df in dataframes[1:]:
            combined = combined.merge(df, on=[id_col, "date"], how="outer")

        return combined

    def _process_all_csv_locations(
        self, df: pd.DataFrame, country: str, has_ext_id: bool
    ) -> pd.DataFrame:
        """
        Process all locations found in the CSV file.

        Args:
            df: DataFrame with CSV data
            country: Country name for validation
            has_ext_id: Whether CSV uses 'ext_id' instead of 'id'

        Returns:
            DataFrame with processed data including location information
        """
        info("Processing all locations from CSV", component="csv_client")

        if has_ext_id:
            # Get unique ext_ids from CSV
            unique_ext_ids = df["ext_id"].unique()
            info(
                f"Found {len(unique_ext_ids)} unique ext_ids in CSV",
                component="csv_client",
                ext_ids=unique_ext_ids.tolist(),
            )

            # Map ext_ids to location_ids from database
            location_mapping = self._get_location_mapping_by_ext_id(
                unique_ext_ids, country
            )

            if not location_mapping:
                error(
                    "No matching locations found in database for CSV ext_ids",
                    component="csv_client",
                )
                return pd.DataFrame()

            # Add location_id to dataframe
            df["location_id"] = df["ext_id"].map(location_mapping)

            # Remove rows where location_id is null (no match in database)
            df = df.dropna(subset=["location_id"])
            df["location_id"] = df["location_id"].astype(int)

        else:
            # CSV has 'id' column, rename to 'location_id' and validate
            df = df.rename(columns={"id": "location_id"})
            unique_location_ids = df["location_id"].unique()
            info(
                f"Found {len(unique_location_ids)} unique location_ids in CSV",
                component="csv_client",
                location_ids=unique_location_ids.tolist(),
            )

            # Validate locations exist in database
            valid_locations = self._validate_locations_in_db(
                unique_location_ids, country
            )

            if not valid_locations:
                error(
                    "No valid locations found in database for CSV location_ids",
                    component="csv_client",
                )
                return pd.DataFrame()

            # Filter to only valid locations
            df = df[df["location_id"].isin(valid_locations)]

        # Enrich with location information
        df = self._enrich_with_location_info(df, country)

        info(
            f"Processed all CSV locations: {len(df)} records for {df['location_id'].nunique()} locations",
            component="csv_client",
        )

        return df

    def _process_specific_locations(
        self, df: pd.DataFrame, location_ids: str, country: str, has_ext_id: bool
    ) -> pd.DataFrame:
        """
        Process specific locations from CSV based on provided location IDs.

        Args:
            df: DataFrame with CSV data
            location_ids: Comma-separated location IDs
            country: Country name for validation
            has_ext_id: Whether CSV uses 'ext_id' instead of 'id'

        Returns:
            DataFrame with processed data for specified locations only
        """
        try:
            # Parse requested location IDs
            requested_ids = [int(lid.strip()) for lid in location_ids.split(",")]
            info(
                f"Processing specific locations: {requested_ids}",
                component="csv_client",
            )

            if has_ext_id:
                # Need to map location_ids to ext_ids
                # First get the locations from database to get their ext_ids
                locations = self.db_manager.get_locations_by_ids(location_ids, country)

                if not locations:
                    error(
                        f"None of the requested location IDs found in database: {requested_ids}",
                        component="csv_client",
                    )
                    return pd.DataFrame()

                # Map location_id to ext_id
                ext_id_mapping = {loc.id: loc.ext_id for loc in locations if loc.ext_id}
                requested_ext_ids = list(ext_id_mapping.values())

                info(
                    f"Mapped location_ids to ext_ids: {ext_id_mapping}",
                    component="csv_client",
                )

                # Filter CSV by ext_ids
                df = df[df["ext_id"].isin(requested_ext_ids)]

                if df.empty:
                    warning(
                        "None of the requested locations found in CSV by ext_id",
                        component="csv_client",
                        requested_ext_ids=requested_ext_ids,
                    )
                    return pd.DataFrame()

                # Map ext_id back to location_id
                reverse_mapping = {
                    ext_id: loc_id for loc_id, ext_id in ext_id_mapping.items()
                }
                df["location_id"] = df["ext_id"].map(reverse_mapping)

            else:
                # CSV has 'id' column - rename to 'location_id' first
                df = df.rename(columns={"id": "location_id"})

                # Validate requested locations exist in database
                locations = self.db_manager.get_locations_by_ids(location_ids, country)

                if not locations:
                    error(
                        f"None of the requested location IDs found in database: {requested_ids}",
                        component="csv_client",
                    )
                    return pd.DataFrame()

                valid_location_ids = [loc.id for loc in locations]

                # Filter CSV by requested location_ids
                df = df[df["location_id"].isin(valid_location_ids)]

                if df.empty:
                    warning(
                        "None of the requested locations found in CSV",
                        component="csv_client",
                        requested_ids=requested_ids,
                    )
                    return pd.DataFrame()

                # Check which requested IDs are missing from CSV
                found_ids = df["location_id"].unique()
                missing_ids = set(valid_location_ids) - set(found_ids)

                if missing_ids:
                    warning(
                        f"Some requested locations not found in CSV: {missing_ids}",
                        component="csv_client",
                    )

            # Enrich with location information
            df = self._enrich_with_location_info(df, country)

            info(
                f"Processed specific locations: {len(df)} records for {df['location_id'].nunique()} locations",
                component="csv_client",
                found_locations=df["location_id"].unique().tolist(),
            )

            return df

        except ValueError as e:
            error(
                f"Invalid location IDs format: {location_ids}",
                component="csv_client",
                error=str(e),
            )
            raise

    def _get_location_mapping_by_ext_id(
        self, ext_ids: List[str], country: str
    ) -> Dict[str, int]:
        """
        Get mapping from ext_id to location_id from database.

        Args:
            ext_ids: List of external IDs from CSV
            country: Country name for filtering

        Returns:
            Dictionary mapping ext_id to location_id
        """
        try:
            # Get all locations for the country
            all_locations = self.db_manager.get_all_locations(country)
            # Build mapping for ext_ids present in CSV
            mapping = {}
            for location in all_locations:
                if location.ext_id and str(location.ext_id) in [
                    str(eid) for eid in ext_ids
                ]:
                    mapping[str(location.ext_id)] = location.id

            info(
                f"Created ext_id to location_id mapping: {len(mapping)} matches",
                component="csv_client",
            )

            return mapping

        except Exception as e:
            error(
                f"Error creating location mapping: {str(e)}",
                component="csv_client",
                error=str(e),
            )
            return {}

    def _validate_locations_in_db(
        self, location_ids: List[int], country: str
    ) -> List[int]:
        """
        Validate that location IDs exist in database for the country.

        Args:
            location_ids: List of location IDs from CSV
            country: Country name for filtering

        Returns:
            List of valid location IDs
        """
        try:
            all_locations = self.db_manager.get_all_locations(country)
            valid_ids = [loc.id for loc in all_locations if loc.id in location_ids]

            invalid_ids = set(location_ids) - set(valid_ids)
            if invalid_ids:
                warning(
                    f"Some location IDs from CSV not found in database: {invalid_ids}",
                    component="csv_client",
                )

            info(
                f"Validated locations: {len(valid_ids)} valid out of {len(location_ids)}",
                component="csv_client",
            )

            return valid_ids

        except Exception as e:
            error(
                f"Error validating locations: {str(e)}",
                component="csv_client",
                error=str(e),
            )
            return []

    def _enrich_with_location_info(
        self, df: pd.DataFrame, country: str
    ) -> pd.DataFrame:
        """
        Enrich DataFrame with location information from database.

        Args:
            df: DataFrame with location_id column
            country: Country name

        Returns:
            DataFrame enriched with location name, latitude, longitude
        """
        try:
            unique_location_ids = df["location_id"].unique()
            locations = {}

            for loc_id in unique_location_ids:
                location = self.db_manager.get_location_info(int(loc_id))
                if location:
                    locations[loc_id] = {
                        "location_name": location.name,
                        "latitude": location.latitude,
                        "longitude": location.longitude,
                    }

            # Add location info to dataframe
            df["location_name"] = df["location_id"].map(
                lambda x: locations.get(x, {}).get("location_name", "Unknown")
            )
            df["latitude"] = df["location_id"].map(
                lambda x: locations.get(x, {}).get("latitude", None)
            )
            df["longitude"] = df["location_id"].map(
                lambda x: locations.get(x, {}).get("longitude", None)
            )

            info(
                f"Enriched data with location information for {len(locations)} locations",
                component="csv_client",
            )

            return df

        except Exception as e:
            error(
                f"Error enriching with location info: {str(e)}",
                component="csv_client",
                error=str(e),
            )
            return df
