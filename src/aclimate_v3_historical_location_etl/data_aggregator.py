"""
Data aggregator module for calculating monthly aggregations from daily data.
This module handles the calculation of monthly statistics from daily point data.
"""

import pandas as pd

from .tools.logging_manager import error, info, warning


class DataAggregatorError(Exception):
    """Custom exception for data aggregation operations"""

    pass


class DataAggregator:
    """Handles aggregation of daily data to monthly statistics."""

    def __init__(self) -> None:
        """Initialize data aggregator."""
        info("Data aggregator initialized", component="data_aggregator")

    def calculate_monthly_aggregations(self, daily_data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate monthly aggregations (averages) from daily data DataFrame.

        Args:
            daily_data: DataFrame with daily climate data

        Returns:
            DataFrame containing monthly aggregations by location, year, and month
        """
        try:
            info(
                "Calculating monthly aggregations from daily data",
                component="data_aggregator",
                total_records=len(daily_data),
            )

            if daily_data.empty:
                warning(
                    "No daily data provided for aggregation",
                    component="data_aggregator",
                )
                return pd.DataFrame()

            # Convert date column to datetime if it's not already
            daily_data["date"] = pd.to_datetime(daily_data["date"])

            # Add year and month columns for grouping
            daily_data["year"] = daily_data["date"].dt.year
            daily_data["month"] = daily_data["date"].dt.month

            # Define metadata columns that should not be aggregated
            metadata_columns = {
                "location_id",
                "location_name",
                "latitude",
                "longitude",
                "date",
                "year",
                "month",
            }

            # Find climate variable columns (numeric columns that are not metadata)
            climate_columns = []
            for col in daily_data.columns:
                if col not in metadata_columns and pd.api.types.is_numeric_dtype(
                    daily_data[col]
                ):
                    climate_columns.append(col)

            info(
                f"Found climate variables for aggregation: {climate_columns}",
                component="data_aggregator",
                climate_variables=climate_columns,
            )

            # Group by location, year, and month, then calculate appropriate aggregation
            grouping_columns = [
                "location_id",
                "location_name",
                "latitude",
                "longitude",
                "year",
                "month",
            ]

            # Prepare aggregation dictionary based on variable type
            agg_dict = {}
            for col in climate_columns:
                # Precipitation variables should be summed (accumulated)
                if "prec" in col.lower() or "precipitation" in col.lower():
                    agg_dict[col] = "sum"
                    info(
                        f"Using SUM aggregation for precipitation variable: {col}",
                        component="data_aggregator",
                        variable=col,
                        aggregation_type="sum",
                    )
                else:
                    # Temperature, radiation, etc. should be averaged
                    agg_dict[col] = "mean"
                    info(
                        f"Using MEAN aggregation for variable: {col}",
                        component="data_aggregator",
                        variable=col,
                        aggregation_type="mean",
                    )

            # Calculate monthly aggregations
            monthly_data = (
                daily_data.groupby(grouping_columns).agg(agg_dict).reset_index()
            )

            # Round the aggregated values to reasonable precision
            for col in climate_columns:
                monthly_data[col] = monthly_data[col].round(2)

            # Create a date column for the first day of each month
            monthly_data["date"] = pd.to_datetime(
                monthly_data[["year", "month"]].assign(day=1)
            )

            info(
                "Monthly aggregations calculated successfully",
                component="data_aggregator",
                monthly_records=len(monthly_data),
                climate_variables=climate_columns,
            )

            return monthly_data

        except Exception as e:
            error(
                "Failed to calculate monthly aggregations",
                component="data_aggregator",
                error=str(e),
            )
            raise DataAggregatorError(
                f"Failed to calculate monthly aggregations: {str(e)}"
            )
