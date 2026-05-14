"""
IELS — Fecha Estimada de Inicio de la Época Seca (Dry Season Onset).

A single calculation process produces three outputs saved to
climate_historical_indicator for each (year, station):

  - IELS          : first julian day j* (May 1 – Nov 30) that satisfies
                    both dry-season detection conditions.
  - IELS-Anomalie : j* minus the station's mean j* over the 1991-2020 norm
                    period (positive = later onset, negative = earlier onset).
  - IELS-decade   : month*10 + decade_within_month of j* (e.g. 51 = May D1,
                    92 = Sep D2). Decode: month=v//10, dec=v%10.

Detection criterion (both must hold simultaneously):
  (1) P10(d0) = sum(P_d, j* to j*+9)                        < 5 mm
  (2) DSC(j*) = count(P_d^(j*+k) < 1 mm, k=0..19)          >= 15 days

Missing precipitation days are treated as 0 (partial sum / dry day).

The three corresponding mng_indicators rows (short_name = "IELS",
"IELS-Anomalie", "IELS-decade") must exist and be enabled in the database
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
_SUB_INDICATORS = ("IELS", "IELS-Anomalie", "IELS-decade")

# Climatological norm period (fixed, independent of ETL date range)
_NORM_START = 1991
_NORM_END = 2020

# Search window bounds for j*
# Lower bound (May 1) is the theoretical earliest; the effective minimum is
# _ONSET_MIN_MONTH (August) to avoid confusing the canícula dry spell with the
# true dry-season onset.  Change _ONSET_MIN_MONTH to adjust per-country.
_SEARCH_START_MONTH = 5  # May  — theoretical lower bound (kept for reference)
_SEARCH_END_MONTH = 11  # November
_ONSET_MIN_MONTH = (
    8  # August — effective minimum for j* to avoid canícula false positives
)


def _is_leap(year: int) -> bool:
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def _may_1_julian(year: int) -> int:
    """Return the 1-based julian day of May 1 for the given year."""
    return date(year, 5, 1).timetuple().tm_yday


def _onset_min_julian(year: int) -> int:
    """Return the 1-based julian day of the effective search start (_ONSET_MIN_MONTH 1)."""
    return date(year, _ONSET_MIN_MONTH, 1).timetuple().tm_yday


def _nov_30_julian(year: int) -> int:
    """Return the 1-based julian day of November 30 for the given year."""
    return date(year, 11, 30).timetuple().tm_yday


class IELSCalculator(BaseIndicatorCalculator):
    """
    Calculator for the IELS dry-season-onset indicator group.

    INDICATOR_CODE = "IELS" matches the short_name used in
    MngCountryIndicator.criteria to activate this calculator.
    """

    INDICATOR_CODE = "IELS"
    SUPPORTED_TEMPORALITIES = ["annual"]
    SECONDARY_CODES = ["IELS-Anomalie", "IELS-decade"]

    def __init__(
        self,
        indicator_config: Dict[str, Any],
        start_date: str,
        end_date: str,
        country_code: str,
    ) -> None:
        super().__init__(indicator_config, start_date, end_date, country_code)
        self._max_workers = int(os.getenv("MAX_PARALLEL_WORKERS", 4))
        self._indicator_meta: Dict[str, Any] = {}
        self._norm_jstar: Dict[int, float] = {}
        self._resolve_sub_indicators()

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def _resolve_sub_indicators(self) -> None:
        """Load the three IELS indicator rows from mng_indicators by short_name."""
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
                "Create them before running the IELS calculator."
            )

        info(
            "IELS sub-indicators resolved from DB",
            component="iels_calculator",
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
            "Fetching precipitation data for IELS",
            component="iels_calculator",
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
            warning("No precipitation data returned", component="iels_calculator")
            return False

        info(
            "Years with data available for IELS",
            component="iels_calculator",
            years_available=sorted(yearly_data.keys()),
            target_years_missing=[y for y in target_years if y not in yearly_data],
        )

        self._build_norm(yearly_data, norm_years)

        results: Dict[str, Dict[int, Dict[int, float]]] = {
            "IELS": {},
            "IELS-Anomalie": {},
            "IELS-decade": {},
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
                        results["IELS"][year] = year_result["IELS"]
                        results["IELS-Anomalie"][year] = year_result["IELS-Anomalie"]
                        results["IELS-decade"][year] = year_result["IELS-decade"]
                        info(
                            "IELS year summary",
                            component="iels_calculator",
                            year=year,
                            stations_onset=len(year_result["IELS"]),
                            stations_anomalie=len(year_result["IELS-Anomalie"]),
                        )
                except Exception as exc:
                    warning(
                        "IELS year processing error",
                        component="iels_calculator",
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

        norm_years_available = [y for y in norm_years if yearly_data.get(y) is not None]
        if len(norm_years_available) < 10:
            warning(
                "IELS norm built from very few years — IELS-Anomalie values will be "
                "unreliable. Load the full 1991–2020 baseline for accurate anomalies.",
                component="iels_calculator",
                norm_years_available=norm_years_available,
            )

        info(
            "IELS 1991-2020 norm computed",
            component="iels_calculator",
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
        if df is None or df.empty:
            warning(
                "No data for year — skipping",
                component="iels_calculator",
                year=year,
            )
            return None

        iels_vals: Dict[int, float] = {}
        anomalie_vals: Dict[int, float] = {}
        decade_vals: Dict[int, float] = {}

        for loc_id, group in df.groupby("location_id"):
            series = self._to_julian_series(group, year)
            j = self._find_jstar(series, year)
            if j is None:
                warning(
                    "No dry season onset found in May–Nov window — station skipped",
                    component="iels_calculator",
                    year=year,
                    loc_id=int(loc_id),
                )
                continue

            iels_vals[int(loc_id)] = float(j)
            decade_vals[int(loc_id)] = float(self._get_decade(j, year))

            norm_mean = self._norm_jstar.get(loc_id)
            if norm_mean is not None:
                anomalie_vals[int(loc_id)] = float(j) - norm_mean

        return {
            "IELS": iels_vals,
            "IELS-Anomalie": anomalie_vals,
            "IELS-decade": decade_vals,
        }

    # ------------------------------------------------------------------
    # Core algorithm helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_julian_series(group: pd.DataFrame, year: int) -> pd.Series:
        """
        Convert a station's daily rows to a Series indexed by julian day (1-based).
        Negative values are clipped to 0. Days with no data remain NaN.
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
        Return the first julian day j* in [Aug 1, Nov 30] where both conditions
        hold simultaneously:

          (1) sum(P[j*..j*+9])                < 5 mm
          (2) count(P[j*+k] < 1 mm, k=0..19) >= 15 days  (observed days only)

        The search starts from August 1 (_ONSET_MIN_MONTH) rather than May 1
        to avoid confusing the canícula (July–August) with the dry-season onset.
        No persistence guard is applied: the August lower bound is sufficient
        to exclude canícula false positives, and the guard was causing valid
        late-October / November onsets to be rejected.

        NaN is not treated as a dry day; only observed values < 1 mm count.
        Returns None if no qualifying day exists in the search window.
        """
        search_start = _onset_min_julian(year) - 1  # 0-based index, Aug 1
        search_end = _nov_30_julian(year) - 1  # 0-based index, Nov 30
        values = series.to_numpy(dtype=float)
        n = len(values)

        # Need 20 days for the detection window to fit within the array.
        max_start = min(search_end, n - 20)

        for i in range(search_start, max_start + 1):
            window10 = values[i : i + 10]
            window20 = values[i : i + 20]

            # Condition 1: 10-day precipitation sum < 5 mm
            p10 = float(np.nansum(window10))
            if p10 >= 5.0:
                continue

            # Condition 2: at least 15 observed dry days (< 1 mm) in 20-day window
            dry_days = int(np.sum((~np.isnan(window20)) & (window20 < 1.0)))
            if dry_days < 15:
                continue

            return i + 1  # 1-based julian day

        return None

    @staticmethod
    def _get_decade(julian_day: int, year: int) -> int:
        """Return month*10 + decade_within_month (e.g. 51=May D1, 92=Sep D2)."""
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
            "IELS results saved to DB",
            component="iels_calculator",
            saved=saved,
            total=len(records),
        )
        return bool(saved == len(records))
