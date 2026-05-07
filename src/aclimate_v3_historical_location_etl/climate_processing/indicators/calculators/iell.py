"""
IELL — Fecha Estimada de Inicio de la Época Lluviosa (Rainy Season Onset).

A single calculation process produces three outputs saved to
climate_historical_indicator for each (year, station):

  - IELL          : first julian day j* (≥ March 1) that satisfies the
                    three consecutive 10-day precipitation conditions.
  - IELL-Anomalie : j* minus the station's mean j* over the 1991-2020 norm
                    period (positive = later onset, negative = earlier onset).
  - IELL-decade   : month*10 + decade_within_month of j* (e.g. 31 = Mar D1,
                    42 = Apr D2, 53 = May D3). Decode: month=v//10, dec=v%10.

Detection criterion (all three must hold simultaneously):
  (1) sum(P_d, j* to j*+9)   >= 20 mm
  (2) sum(P_d, j*+10 to j*+19) >  0 mm
  (3) sum(P_d, j*+20 to j*+29) >  0 mm

Missing precipitation days are treated as 0 (partial sum).

The three corresponding mng_indicators rows (short_name = "IELL",
"IELL-Anomalie", "IELL-decade") must exist and be enabled in the database
before this calculator is invoked.
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from aclimate_v3_orm.enums import Period
from aclimate_v3_orm.schemas import ClimateHistoricalIndicatorCreate
from aclimate_v3_orm.services import (
    ClimateHistoricalIndicatorService,
    MngIndicatorService,
)

from ....tools.logging_manager import info, warning
from ..base_calculator import BaseIndicatorCalculator
from ..data_fetcher import IndicatorDataFetcher

# Short names of the three indicators that must be present in mng_indicators
_SUB_INDICATORS = ("IELL", "IELL-Anomalie", "IELL-decade")

# Climatological norm period (fixed, independent of ETL date range)
_NORM_START = 1991
_NORM_END = 2020

# Julian day of March 1 in a non-leap year  (31 Jan + 28 Feb = 59, so day 60)
_MARCH_1_NOLEAP = 60


def _is_leap(year: int) -> bool:
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def _march_1_julian(year: int) -> int:
    """Return the 1-based julian day of March 1 for the given year."""
    return _MARCH_1_NOLEAP + (1 if _is_leap(year) else 0)


class IELLCalculator(BaseIndicatorCalculator):
    """
    Calculator for the IELL rainy-season-onset indicator group.

    INDICATOR_CODE = "IELL" matches the short_name used in
    MngCountryIndicator.criteria to activate this calculator.
    """

    INDICATOR_CODE = "IELL"
    SUPPORTED_TEMPORALITIES = ["annual"]
    # IELL-Anomalie and IELL-decade are sub-outputs of this same calculator.
    # They must exist in mng_indicators but do NOT need their own calculator class.
    SECONDARY_CODES = ["IELL-Anomalie", "IELL-decade"]

    def __init__(
        self,
        indicator_config: Dict[str, Any],
        start_date: str,
        end_date: str,
        country_code: str,
    ) -> None:
        super().__init__(indicator_config, start_date, end_date, country_code)
        self._max_workers = int(os.getenv("MAX_PARALLEL_WORKERS", 4))
        # {short_name: IndicatorRead}  populated by _resolve_sub_indicators
        self._indicator_meta: Dict[str, Any] = {}
        # {location_id: mean_jstar_1991_2020}  populated by _build_norm
        self._norm_jstar: Dict[int, float] = {}
        self._resolve_sub_indicators()

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def _resolve_sub_indicators(self) -> None:
        """Load the three IELL indicator rows from mng_indicators by short_name."""
        svc = MngIndicatorService()
        missing: List[str] = []
        for sn in _SUB_INDICATORS:
            rows = svc.get_by_short_name(sn)
            if not rows:
                missing.append(sn)
            else:
                self._indicator_meta[sn] = rows[0]

        if missing:
            raise ValueError(
                f"Required indicators not found in mng_indicators: {missing}. "
                "Create them before running the IELL calculator."
            )

        info(
            "IELL sub-indicators resolved from DB",
            component="iell_calculator",
            ids={sn: meta.id for sn, meta in self._indicator_meta.items()},
        )

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    def calculate_annual(self) -> bool:
        start_year = int(self.start_date.split("-")[0])
        end_year = int(self.end_date.split("-")[0])

        target_years: List[int] = list(range(start_year, end_year + 1))
        norm_years: List[int] = list(range(_NORM_START, _NORM_END + 1))
        all_years: List[int] = sorted(set(target_years) | set(norm_years))

        info(
            "Fetching precipitation data for IELL",
            component="iell_calculator",
            country=self.country_code,
            fetch_range=f"{all_years[0]}-{all_years[-1]}",
            target_range=f"{start_year}-{end_year}",
        )

        fetcher = IndicatorDataFetcher(
            country_code=self.country_code,
            variable="prec",
            year_range=(str(all_years[0]), str(all_years[-1])),
            parallel_workers=self._max_workers,
        )
        yearly_data: Dict[int, pd.DataFrame] = fetcher.fetch_all_years()

        if not yearly_data:
            warning("No precipitation data returned", component="iell_calculator")
            return False

        # Build the per-station norm (mean j* over 1991-2020)
        self._build_norm(yearly_data, norm_years)

        # Process target years in parallel
        results: Dict[str, Dict[int, Dict[int, float]]] = {
            "IELL": {},
            "IELL-Anomalie": {},
            "IELL-decade": {},
        }

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {
                executor.submit(self._process_year, year, yearly_data.get(year)): year
                for year in target_years
            }
            for future in as_completed(futures):
                year = futures[future]
                try:
                    year_result = future.result()
                    if year_result:
                        results["IELL"][year] = year_result["IELL"]
                        results["IELL-Anomalie"][year] = year_result["IELL-Anomalie"]
                        results["IELL-decade"][year] = year_result["IELL-decade"]
                except Exception as exc:
                    warning(
                        "IELL year processing error",
                        component="iell_calculator",
                        year=year,
                        error=str(exc),
                    )

        return self._save_results_to_db(results)

    # ------------------------------------------------------------------
    # Norm computation
    # ------------------------------------------------------------------

    def _build_norm(
        self,
        yearly_data: Dict[int, pd.DataFrame],
        norm_years: List[int],
    ) -> None:
        """Compute per-station mean j* over the 1991-2020 norm period."""
        jstar_by_station: Dict[int, List[int]] = {}

        for year in norm_years:
            df = yearly_data.get(year)
            if df is None or df.empty:
                continue
            for loc_id, group in df.groupby("location_id"):
                series = self._to_julian_series(group, year)
                j = self._find_jstar(series, year)
                if j is not None:
                    jstar_by_station.setdefault(loc_id, []).append(j)

        self._norm_jstar = {
            loc_id: float(np.mean(vals))
            for loc_id, vals in jstar_by_station.items()
            if vals
        }

        info(
            "IELL 1991-2020 norm computed",
            component="iell_calculator",
            stations_with_norm=len(self._norm_jstar),
        )

    # ------------------------------------------------------------------
    # Per-year calculation
    # ------------------------------------------------------------------

    def _process_year(
        self,
        year: int,
        df: Optional[pd.DataFrame],
    ) -> Optional[Dict[str, Dict[int, float]]]:
        """
        Find j*, decade, and anomaly for every station in one year.

        Returns:
            Dict with keys "IELL", "IELL-Anomalie", "IELL-decade", each
            mapping location_id -> value.  Returns None if df is empty.
        """
        if df is None or df.empty:
            return None

        iell_vals: Dict[int, float] = {}
        anomalie_vals: Dict[int, float] = {}
        decade_vals: Dict[int, float] = {}

        for loc_id, group in df.groupby("location_id"):
            series = self._to_julian_series(group, year)
            j = self._find_jstar(series, year)
            if j is None:
                continue

            iell_vals[int(loc_id)] = float(j)
            decade_vals[int(loc_id)] = float(self._get_decade(j, year))

            norm_mean = self._norm_jstar.get(loc_id)
            if norm_mean is not None:
                anomalie_vals[int(loc_id)] = float(j) - norm_mean

        return {
            "IELL": iell_vals,
            "IELL-Anomalie": anomalie_vals,
            "IELL-decade": decade_vals,
        }

    # ------------------------------------------------------------------
    # Core algorithm helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_julian_series(group: pd.DataFrame, year: int) -> pd.Series:
        """
        Convert a station's daily rows to a Series indexed by julian day (1-based).

        Negative precipitation values are clipped to 0.
        Days with no data remain NaN.
        """
        g = group.copy()
        g["date"] = pd.to_datetime(g["date"])
        g["julian"] = g["date"].dt.dayofyear
        g["value"] = g["value"].clip(lower=0)
        series = g.groupby("julian")["value"].sum()
        total_days = 366 if _is_leap(year) else 365
        return series.reindex(range(1, total_days + 1))

    @staticmethod
    def _find_jstar(series: pd.Series, year: int) -> Optional[int]:
        """
        Return the first julian day j* >= March 1 where all three consecutive
        10-day precipitation conditions are satisfied.

        NaN values are treated as 0 mm (partial sum).
        Returns None if no qualifying day exists.
        """
        start_idx = _march_1_julian(year) - 1  # convert to 0-based index
        values = series.to_numpy(dtype=float)  # NaN preserved as float NaN
        n = len(values)

        for i in range(start_idx, n - 29):
            d0 = float(np.nansum(values[i : i + 10]))
            d1 = float(np.nansum(values[i + 10 : i + 20]))
            d2 = float(np.nansum(values[i + 20 : i + 30]))
            if d0 >= 20.0 and d1 > 0.0 and d2 > 0.0:
                return i + 1  # back to 1-based julian day

        return None

    @staticmethod
    def _get_decade(julian_day: int, year: int) -> int:
        """Return month*10 + decade_within_month (e.g. 31=Mar D1, 42=Apr D2, 53=May D3)."""
        d = date(year, 1, 1) + timedelta(days=julian_day - 1)
        if d.day <= 10:
            dec = 1
        elif d.day <= 20:
            dec = 2
        else:
            dec = 3
        return d.month * 10 + dec

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save_results_to_db(
        self,
        results: Dict[str, Dict[int, Dict[int, float]]],
    ) -> bool:
        """
        Persist all three indicator groups to climate_historical_indicator.

        Args:
            results: {short_name -> {year -> {location_id -> value}}}

        Returns:
            True if no records failed to save.
        """
        svc = ClimateHistoricalIndicatorService()
        records: List[ClimateHistoricalIndicatorCreate] = []

        for short_name, yearly in results.items():
            meta = self._indicator_meta.get(short_name)
            if meta is None:
                continue
            indicator_id: int = meta.id

            for year, loc_values in yearly.items():
                start_dt = date(year, 1, 1)
                end_dt = date(year, 12, 31)

                for loc_id, value in loc_values.items():
                    records.append(
                        ClimateHistoricalIndicatorCreate(
                            indicator_id=indicator_id,
                            location_id=loc_id,
                            value=value,
                            period=Period.ANNUAL,
                            start_date=start_dt,
                            end_date=end_dt,
                        )
                    )

        saved = svc.bulk_create(records)

        info(
            "IELL results saved to DB",
            component="iell_calculator",
            saved=saved,
            total=len(records),
        )
        return bool(saved == len(records))
