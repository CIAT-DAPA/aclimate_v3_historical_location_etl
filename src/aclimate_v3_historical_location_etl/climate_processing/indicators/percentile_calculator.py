from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any

import numpy as np
import pandas as pd

from .base_calculator import BaseIndicatorCalculator
from .data_fetcher import IndicatorDataFetcher
from ...tools.logging_manager import error, info, warning


class PercentileBasedCalculator(BaseIndicatorCalculator, ABC):
    """
    Base class for percentile-based climate indicators (point/station level).

    Handles the common workflow for indicators that require calculating
    percentiles over a base period (e.g., TX90p, TX10p, R95pTOT).

    Features:
    - Centralised base period configuration per data type
    - Shared percentile cache at class level (across instances, per country+variable)
    - Shared base-period data cache to avoid duplicate downloads
    """

    # Override in concrete subclasses if a different base period is needed
    BASE_PERIODS: Dict[str, Dict[str, str]] = {
        "temperature": {"start": "1981", "end": "2010"},
        "precipitation": {"start": "1981", "end": "2010"},
    }

    # Class-level caches — shared across all instances of the same subclass
    # {cache_key: {percentile_int: {location_id: float}}}
    _percentile_cache: Dict[str, Dict[int, Dict[int, float]]] = {}

    # {cache_key: {year: pd.DataFrame}}
    _base_period_data_cache: Dict[str, Dict[int, pd.DataFrame]] = {}

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._cache_key: Optional[str] = None

    # ------------------------------------------------------------------
    # Abstract properties — concrete subclasses must define these
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def required_percentiles(self) -> List[int]:
        """List of integer percentile values needed, e.g. [90] or [95]."""
        pass

    @property
    @abstractmethod
    def data_type(self) -> str:
        """Data type key used for BASE_PERIODS lookup: 'temperature' or 'precipitation'."""
        pass

    @property
    @abstractmethod
    def data_variable(self) -> str:
        """Climate measure short_name used to query the DB (e.g., 'tmax', 'prec')."""
        pass

    @property
    @abstractmethod
    def data_source_key(self) -> str:
        """Key used to instantiate IndicatorDataFetcher (same as data_variable in most cases)."""
        pass

    # ------------------------------------------------------------------
    # Base period helpers
    # ------------------------------------------------------------------

    @property
    def base_period_start(self) -> str:
        return self.BASE_PERIODS[self.data_type]["start"]

    @property
    def base_period_end(self) -> str:
        return self.BASE_PERIODS[self.data_type]["end"]

    def _get_cache_key(self) -> str:
        """Generate a unique percentile cache key for this calculator configuration."""
        if self._cache_key is None:
            percentiles_str = "_".join(str(p) for p in sorted(self.required_percentiles))
            self._cache_key = (
                f"{self.country_code}_{self.data_variable}"
                f"_{percentiles_str}"
                f"_{self.base_period_start}_{self.base_period_end}"
            )
        return self._cache_key

    def _get_base_data_cache_key(self) -> str:
        """Generate a unique key for the base-period DataFrames cache."""
        return (
            f"{self.country_code}_{self.data_variable}"
            f"_{self.base_period_start}_{self.base_period_end}"
        )

    # ------------------------------------------------------------------
    # Percentile retrieval (with caching)
    # ------------------------------------------------------------------

    def get_base_period_percentiles(
        self,
    ) -> Optional[Dict[int, Dict[int, float]]]:
        """
        Return base-period percentiles, calculating and caching them on first call.

        Returns:
            {percentile_int: {location_id: float}} or None on failure
        """
        cache_key = self._get_cache_key()

        if cache_key in self._percentile_cache:
            info(
                "Using cached base period percentiles",
                component=f"{self.INDICATOR_CODE.lower()}_calculator",
                cache_key=cache_key,
            )
            return self._percentile_cache[cache_key]

        info(
            f"Calculating base period percentiles for {self.INDICATOR_CODE}",
            component=f"{self.INDICATOR_CODE.lower()}_calculator",
            percentiles=self.required_percentiles,
            data_type=self.data_type,
            base_period=f"{self.base_period_start}-{self.base_period_end}",
        )

        percentiles_dict = self._calculate_base_period_percentiles()

        if percentiles_dict is not None:
            self._percentile_cache[cache_key] = percentiles_dict
            info(
                "Base period percentiles cached",
                component=f"{self.INDICATOR_CODE.lower()}_calculator",
                cache_key=cache_key,
                percentiles_calculated=list(percentiles_dict.keys()),
            )

        return percentiles_dict

    def _calculate_base_period_percentiles(
        self,
    ) -> Optional[Dict[int, Dict[int, float]]]:
        """
        Download base-period data and compute per-station percentiles.

        Returns:
            {percentile_int: {location_id: float}} or None on failure
        """
        try:
            base_data_key = self._get_base_data_cache_key()
            base_years = list(range(int(self.base_period_start), int(self.base_period_end) + 1))

            # Populate base period data cache if needed
            if base_data_key not in self._base_period_data_cache:
                fetcher = IndicatorDataFetcher(
                    country_code=self.country_code,
                    variable=self.data_source_key,
                    year_range=(self.base_period_start, self.base_period_end),
                )
                datasets = fetcher.fetch_all_years()
                self._base_period_data_cache[base_data_key] = datasets
            else:
                datasets = self._base_period_data_cache[base_data_key]

            if not datasets:
                error(
                    "No base period data available for percentile calculation",
                    component=f"{self.INDICATOR_CODE.lower()}_calculator",
                    base_period=f"{self.base_period_start}-{self.base_period_end}",
                )
                return None

            # Preprocess each year's DataFrame
            processed_frames = []
            for year in base_years:
                if year not in datasets:
                    continue
                df = self._preprocess_data(datasets[year].copy(), year)
                if df is not None and not df.empty:
                    processed_frames.append(df)

            if not processed_frames:
                error(
                    "No valid base period data after preprocessing",
                    component=f"{self.INDICATOR_CODE.lower()}_calculator",
                )
                return None

            combined = pd.concat(processed_frames, ignore_index=True)

            # Compute each requested percentile per station
            percentiles_result: Dict[int, Dict[int, float]] = {}
            for percentile in self.required_percentiles:
                per_station: Dict[int, float] = {}
                for location_id, group in combined.groupby("location_id"):
                    series = group["value"].dropna()
                    value = self._calculate_percentile_for_station(series, percentile)
                    if value is not None:
                        per_station[int(location_id)] = value
                percentiles_result[percentile] = per_station

            info(
                "Base period percentiles calculated successfully",
                component=f"{self.INDICATOR_CODE.lower()}_calculator",
                percentiles=list(percentiles_result.keys()),
                stations_with_data=len(next(iter(percentiles_result.values()), {})),
            )
            return percentiles_result

        except Exception as e:
            error(
                "Failed to calculate base period percentiles",
                component=f"{self.INDICATOR_CODE.lower()}_calculator",
                error=str(e),
            )
            return None

    # ------------------------------------------------------------------
    # Dataset retrieval for indicator period (reuses base-period cache)
    # ------------------------------------------------------------------

    def get_datasets_for_indicator_calculation(
        self, start_year: str, end_year: str
    ) -> Optional[Dict[int, pd.DataFrame]]:
        """
        Get datasets for indicator period, reusing base-period data where available.

        Only downloads years that are not already cached from the base period fetch.

        Args:
            start_year: First year of the indicator calculation range (string)
            end_year: Last year of the indicator calculation range (string)

        Returns:
            {year: DataFrame} or None if fetching fails
        """
        try:
            base_data_key = self._get_base_data_cache_key()
            indicator_years = list(range(int(start_year), int(end_year) + 1))

            cached_datasets = self._base_period_data_cache.get(base_data_key, {})

            # Determine which years still need downloading
            missing_years = [y for y in indicator_years if y not in cached_datasets]

            if missing_years:
                # Group consecutive missing years to minimise fetcher calls
                ranges = self._group_consecutive_years(missing_years)

                for range_start, range_end in ranges:
                    fetcher = IndicatorDataFetcher(
                        country_code=self.country_code,
                        variable=self.data_source_key,
                        year_range=(str(range_start), str(range_end)),
                    )
                    new_data = fetcher.fetch_all_years()
                    # Merge into the class-level base data cache
                    if base_data_key not in self._base_period_data_cache:
                        self._base_period_data_cache[base_data_key] = {}
                    self._base_period_data_cache[base_data_key].update(new_data)

            # Return only the requested indicator years
            all_data = self._base_period_data_cache.get(base_data_key, {})
            result = {y: all_data[y] for y in indicator_years if y in all_data}

            info(
                "Datasets ready for indicator calculation",
                component=f"{self.INDICATOR_CODE.lower()}_calculator",
                indicator_years=indicator_years,
                years_available=sorted(result.keys()),
            )
            return result if result else None

        except Exception as e:
            error(
                "Failed to get datasets for indicator calculation",
                component=f"{self.INDICATOR_CODE.lower()}_calculator",
                error=str(e),
            )
            return None

    def _group_consecutive_years(self, years: List[int]) -> List[tuple]:
        """
        Group a sorted list of years into (start, end) consecutive ranges.

        Args:
            years: Sorted list of integer years

        Returns:
            List of (start_year, end_year) tuples
        """
        if not years:
            return []

        ranges = []
        start = years[0]
        end = years[0]

        for year in years[1:]:
            if year == end + 1:
                end = year
            else:
                ranges.append((start, end))
                start = year
                end = year

        ranges.append((start, end))
        return ranges

    # ------------------------------------------------------------------
    # Abstract preprocessing and percentile calculation hooks
    # ------------------------------------------------------------------

    @abstractmethod
    def _preprocess_data(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Preprocess raw DataFrame for a given year before percentile calculation.

        Args:
            df: DataFrame with columns [location_id, date, value]
            year: The year being processed

        Returns:
            Cleaned DataFrame (same columns, values adjusted as needed)
        """
        pass

    @abstractmethod
    def _calculate_percentile_for_station(
        self, series: pd.Series, percentile: int
    ) -> Optional[float]:
        """
        Calculate a single percentile value from a 1-D series of daily values.

        Args:
            series: Non-null daily values for one station over the base period
            percentile: Integer percentile to calculate (e.g., 90)

        Returns:
            Percentile value as float, or None if calculation fails
        """
        pass

    # ------------------------------------------------------------------
    # Cache management
    # ------------------------------------------------------------------

    @classmethod
    def clear_percentile_cache(cls) -> None:
        """Clear both the percentile cache and the base-period data cache."""
        cls._percentile_cache.clear()
        cls._base_period_data_cache.clear()
        info(
            "Percentile and base period data caches cleared",
            component="percentile_calculator",
        )

    @classmethod
    def get_cache_info(cls) -> dict:
        """Return summary information about the current cache state."""
        return {
            "percentile_cache_keys": list(cls._percentile_cache.keys()),
            "base_period_data_cache_keys": list(cls._base_period_data_cache.keys()),
            "total_percentile_entries": sum(
                len(v) for v in cls._percentile_cache.values()
            ),
        }


# ===========================================================================
# Concrete intermediate classes for temperature and precipitation percentiles
# ===========================================================================


class TemperaturePercentileCalculator(PercentileBasedCalculator):
    """
    Intermediate base for temperature-based percentile indicators (TX90p, TX10p, etc.).

    Subclasses only need to define INDICATOR_CODE, SUPPORTED_TEMPORALITIES,
    required_percentiles, calculate_annual(), and _save_results_to_db().
    """

    @property
    def data_type(self) -> str:
        return "temperature"

    @property
    def data_variable(self) -> str:
        return "tmax"

    @property
    def data_source_key(self) -> str:
        return "tmax"

    def _preprocess_data(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Convert temperature from Kelvin to Celsius if the mean looks like Kelvin (> 200).
        Drops rows with NaN values.
        """
        try:
            df = df.copy()
            mean_val = df["value"].dropna().mean()
            if not np.isnan(mean_val) and mean_val > 200:
                df["value"] = df["value"] - 273.15
                info(
                    "Converted temperature from Kelvin to Celsius",
                    component="temperature_percentile_calculator",
                    year=year,
                    sample_mean_after=float(df["value"].mean()),
                )
            return df
        except Exception as e:
            error(
                "Failed to preprocess temperature data",
                component="temperature_percentile_calculator",
                year=year,
                error=str(e),
            )
            return df

    def _calculate_percentile_for_station(
        self, series: pd.Series, percentile: int
    ) -> Optional[float]:
        """Simple np.nanpercentile across all daily values for this station."""
        try:
            clean = series.dropna()
            if clean.empty:
                return None
            return float(np.nanpercentile(clean.values, percentile))
        except Exception as e:
            error(
                "Failed to calculate temperature percentile",
                component="temperature_percentile_calculator",
                error=str(e),
            )
            return None


class PrecipitationPercentileCalculator(PercentileBasedCalculator):
    """
    Intermediate base for precipitation-based percentile indicators (R95pTOT, etc.).

    Percentile is computed only over wet days (value >= 1 mm).

    Subclasses only need to define INDICATOR_CODE, SUPPORTED_TEMPORALITIES,
    required_percentiles, calculate_annual(), and _save_results_to_db().
    """

    @property
    def data_type(self) -> str:
        return "precipitation"

    @property
    def data_variable(self) -> str:
        return "prec"

    @property
    def data_source_key(self) -> str:
        return "prec"

    def _preprocess_data(self, df: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Clean precipitation data:
        - Replace -9999 and other negative values with NaN
        - Convert m → mm if maximum value < 1 (i.e., data appears to be in metres)
        """
        try:
            df = df.copy()
            # Replace common no-data flags and negative values
            df["value"] = df["value"].where(df["value"] >= 0, other=np.nan)

            # If all non-NaN values are < 1, assume the unit is metres
            max_val = df["value"].dropna().max()
            if not np.isnan(max_val) and max_val < 1:
                df["value"] = df["value"] * 1000.0
                info(
                    "Converted precipitation from m to mm",
                    component="precipitation_percentile_calculator",
                    year=year,
                )
            return df
        except Exception as e:
            error(
                "Failed to preprocess precipitation data",
                component="precipitation_percentile_calculator",
                year=year,
                error=str(e),
            )
            return df

    def _calculate_percentile_for_station(
        self, series: pd.Series, percentile: int
    ) -> Optional[float]:
        """
        Percentile calculated only over wet days (daily value >= 1 mm).
        Returns None if there are no wet days.
        """
        try:
            wet_days = series.dropna()
            wet_days = wet_days[wet_days >= 1.0]
            if wet_days.empty:
                return None
            return float(np.nanpercentile(wet_days.values, percentile))
        except Exception as e:
            error(
                "Failed to calculate precipitation percentile",
                component="precipitation_percentile_calculator",
                error=str(e),
            )
            return None
