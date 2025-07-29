"""
Climatology calculator module for calculating long-term climate statistics.
This module handles the calculation of climatological normals from historical data.
"""

from typing import Any, Dict, List

from aclimate_v3_orm.schemas import ClimateHistoricalClimatologyCreate

from .tools.logging_manager import error, info


class ClimatologyError(Exception):
    """Custom exception for climatology operations"""

    pass


class ClimatologyCalculator:
    """Handles calculation of climatological statistics from historical data."""

    def __init__(self) -> None:
        """
        Initialize climatology calculator.
        """
        info("Climatology calculator initialized", component="climatology_calculator")

    def calculate_monthly_climatology(
        self, historical_data: List[Dict[str, Any]], station_id: int
    ) -> List[ClimateHistoricalClimatologyCreate]:
        """
        Calculate monthly climatological normals for a station
        using all available data (no reference period).
        Returns a list of dicts matching
        the ClimateHistoricalClimatologyBase schema:
        location_id, measure_id, month,
        value (mean for each variable per month).

        Args:
            historical_data: List of monthly historical data
            station_id: Station ID for which to calculate climatology

        Returns:
            List of dicts, each with keys: location_id, measure_id, month, value
        """
        try:
            info(
                "Calculating monthly climatology (all data, flat format)",
                component="climatology_calculator",
                station_id=station_id,
                data_records=len(historical_data),
            )

            # Group all data by month (no filtering)
            monthly_groups = self._group_by_month(historical_data)

            measure_ids = set()
            for record in historical_data:
                mid = record.get("measure_id")
                if mid is not None:
                    measure_ids.add(int(mid))

            climatology_records: list[ClimateHistoricalClimatologyCreate] = []
            for month in range(1, 13):
                if month in monthly_groups:
                    month_data = monthly_groups[month]
                    for measure_id in measure_ids:
                        values = [
                            float(row["value"])
                            for row in month_data
                            if int(row.get("measure_id", 0)) == measure_id
                            and row.get("value") is not None
                        ]
                        if values:
                            mean_value = sum(values) / len(values)
                            climatology_records.append(
                                ClimateHistoricalClimatologyCreate(
                                    location_id=int(station_id),
                                    measure_id=int(measure_id),
                                    month=int(month),
                                    value=float(mean_value),
                                )
                            )

            info(
                f"Monthly climatology calculated successfully (all data, flat format), "
                f"for {len(climatology_records)} records.",
                component="climatology_calculator",
                station_id=station_id,
            )

            return climatology_records

        except Exception as e:
            error(
                "Failed to calculate monthly climatology",
                component="climatology_calculator",
                station_id=station_id,
                error=str(e),
            )
            raise ClimatologyError(f"Failed to calculate monthly climatology: {str(e)}")

    def _group_by_month(self, reference_data: list[dict]) -> dict[int, list[dict]]:
        """Group reference data by month (extracting month from 'date' field)."""
        try:
            monthly_groups: dict[int, list[dict]] = {}

            for record in reference_data:
                # 'date' is a datetime.date object or string in ISO format
                date_value = record.get("date")
                if date_value is None:
                    continue
                # If it's a string, convert to date
                if isinstance(date_value, str):
                    from datetime import datetime

                    date_value = datetime.fromisoformat(date_value).date()
                month = date_value.month
                if month not in monthly_groups:
                    monthly_groups[month] = []
                monthly_groups[month].append(record)

            return monthly_groups

        except Exception as e:
            error(
                "Failed to group data by month",
                component="climatology_calculator",
                error=str(e),
            )
            return {}
