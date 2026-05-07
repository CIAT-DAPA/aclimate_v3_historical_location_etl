import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import Dict, List, Optional, Tuple

import pandas as pd
from aclimate_v3_orm.services import (
    ClimateHistoricalDailyService,
    MngClimateMeasureService,
    MngCountryService,
    MngLocationService,
)

from ...tools.logging_manager import error, info, warning


class IndicatorDataFetcher:
    """
    Fetches daily climate data per station (location) from the project database.

    This class queries the ORM for daily historical data for a given country,
    variable, and year range, returning tidy DataFrames with columns
    [location_id, date, value].
    """

    def __init__(
        self,
        country_code: str,
        variable: str,
        year_range: Tuple[str, str],
        station_ids: Optional[List[int]] = None,
        parallel_workers: int = 4,
    ):
        """
        Initialize the data fetcher.

        Args:
            country_code: ISO2 country code in any case (e.g., "hn", "HN")
            variable: Climate measure short_name (e.g., "tmax", "prec", "tmin")
            year_range: Tuple of (start_year, end_year) strings, e.g. ("2000", "2010")
            station_ids: Optional list of location_id integers to restrict the query.
                         If None, all enabled locations for the country are used.
            parallel_workers: Number of threads for parallel year downloads.
        """
        self.country_code = country_code.upper()
        self.variable = variable
        self.start_year = int(year_range[0])
        self.end_year = int(year_range[1])
        self.parallel_workers = int(os.getenv("MAX_PARALLEL_WORKERS", parallel_workers))

        self._daily_service = ClimateHistoricalDailyService()
        self._measure_service = MngClimateMeasureService()
        self._country_service = MngCountryService()
        self._location_service = MngLocationService()

        self._measure_id: Optional[int] = None
        self._location_ids: Optional[List[int]] = station_ids

        info(
            "IndicatorDataFetcher initialized",
            component="indicator_data_fetcher",
            country_code=self.country_code,
            variable=variable,
            year_range=year_range,
            station_ids_provided=station_ids is not None,
        )

    def _resolve_measure_id(self) -> int:
        """
        Resolve and cache the measure_id for the configured variable short_name.

        Returns:
            int: The measure_id from mng_climate_measure

        Raises:
            ValueError: If no measure is found for the given short_name
        """
        if self._measure_id is not None:
            return self._measure_id

        measures = self._measure_service.get_by_short_name(self.variable)
        if not measures:
            raise ValueError(
                f"No climate measure found with short_name='{self.variable}'"
            )

        self._measure_id = measures[0].id
        info(
            "Resolved measure_id for variable",
            component="indicator_data_fetcher",
            variable=self.variable,
            measure_id=self._measure_id,
        )
        return self._measure_id

    def _resolve_location_ids(self) -> List[int]:
        """
        Resolve and cache the list of location_ids for the country.

        Returns:
            List[int]: Enabled location IDs for the country
        """
        if self._location_ids is not None:
            return self._location_ids

        locations = self._location_service.get_by_country_name(self.country_code)
        if not locations:
            warning(
                "No enabled locations found for country",
                component="indicator_data_fetcher",
                country_code=self.country_code,
            )
            self._location_ids = []
        else:
            self._location_ids = [loc.id for loc in locations]
            info(
                "Resolved location_ids for country",
                component="indicator_data_fetcher",
                country_code=self.country_code,
                location_count=len(self._location_ids),
            )

        return self._location_ids

    def get_station_ids(self) -> List[int]:
        """
        Return the list of location_ids available for the country and variable.

        Returns:
            List[int]: Location IDs (station identifiers)
        """
        return self._resolve_location_ids()

    def fetch_year_data(self, year: int) -> Optional[pd.DataFrame]:
        """
        Fetch all daily data for a single year for the configured country and variable.

        Args:
            year: Integer year to fetch (e.g., 2020)

        Returns:
            DataFrame with columns [location_id, date, value] or None if fetching fails.
            Missing days for a given station are represented as NaN in the value column.
        """
        try:
            measure_id = self._resolve_measure_id()
            location_ids = self._resolve_location_ids()

            if not location_ids:
                warning(
                    "No locations available — skipping year",
                    component="indicator_data_fetcher",
                    year=year,
                )
                return None

            start_date = date(year, 1, 1)
            end_date = date(year, 12, 31)

            info(
                "Fetching daily data for year",
                component="indicator_data_fetcher",
                year=year,
                start_date=str(start_date),
                end_date=str(end_date),
                variable=self.variable,
                measure_id=measure_id,
                location_count=len(location_ids),
            )

            # Fetch all daily records in the date range for the country
            records = self._daily_service.get_by_date_range(start_date, end_date)

            if not records:
                warning(
                    "No daily records returned for year",
                    component="indicator_data_fetcher",
                    year=year,
                )
                return None

            # Build a DataFrame and filter to this measure and these locations
            rows = [
                {
                    "location_id": r.location_id,
                    "date": r.date,
                    "value": r.value,
                }
                for r in records
                if r.measure_id == measure_id and r.location_id in set(location_ids)
            ]

            if not rows:
                warning(
                    "No records matched measure_id and location_ids for year",
                    component="indicator_data_fetcher",
                    year=year,
                    measure_id=measure_id,
                )
                return None

            df = pd.DataFrame(rows)
            df["date"] = pd.to_datetime(df["date"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

            info(
                "Daily data fetched successfully for year",
                component="indicator_data_fetcher",
                year=year,
                records=len(df),
                locations=df["location_id"].nunique(),
            )
            return df

        except Exception as e:
            error(
                "Failed to fetch data for year",
                component="indicator_data_fetcher",
                year=year,
                error=str(e),
            )
            return None

    def fetch_all_years(self) -> Dict[int, pd.DataFrame]:
        """
        Fetch daily data for all years in the configured range using parallel threads.

        Returns:
            Dict mapping year (int) to DataFrame with columns [location_id, date, value].
            Years that fail to fetch are omitted from the result.
        """
        years = list(range(self.start_year, self.end_year + 1))

        info(
            "Fetching data for all years in range",
            component="indicator_data_fetcher",
            start_year=self.start_year,
            end_year=self.end_year,
            total_years=len(years),
            parallel_workers=self.parallel_workers,
        )

        results: Dict[int, pd.DataFrame] = {}

        with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
            future_to_year = {
                executor.submit(self.fetch_year_data, year): year for year in years
            }
            for future in as_completed(future_to_year):
                year = future_to_year[future]
                try:
                    df = future.result()
                    if df is not None and not df.empty:
                        results[year] = df
                    else:
                        warning(
                            "No data returned for year — skipping",
                            component="indicator_data_fetcher",
                            year=year,
                        )
                except Exception as e:
                    error(
                        "Exception while fetching year data",
                        component="indicator_data_fetcher",
                        year=year,
                        error=str(e),
                    )

        info(
            "Completed fetching all years",
            component="indicator_data_fetcher",
            years_fetched=sorted(results.keys()),
            years_missing=[y for y in years if y not in results],
        )
        return results
