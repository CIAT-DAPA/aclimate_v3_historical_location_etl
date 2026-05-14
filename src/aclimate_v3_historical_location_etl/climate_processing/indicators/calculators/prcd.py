"""
PRCD — Precipitación Acumulada Decádica (Decadal Accumulated Precipitation).

A single calculation process produces four outputs saved to
climate_historical_indicator for each (year, station, decade):

  - PRCD      : P10(d) — accumulated precipitation of the decade [mm]
  - PRCD-Abs  : ΔP10_abs = P10(d) − P̄10,n(d)                    [mm]
  - PRCD-Rel  : ΔP10_rel = (P10(d) − P̄10,n(d)) / P̄10,n(d) × 100 [%]
  - PRCD-Cat  : tercile category — 1=Below Normal, 2=Normal, 3=Above Normal

Each year produces up to 36 decades × 4 indicators × N stations records,
stored with period=DECADAL and the exact start/end dates of each decade.

Decades are defined as:
  D1: day  1–10 of the month
  D2: day 11–20 of the month
  D3: day 21–last day of the month

Climatological norms (mean, percentile 33, percentile 67) are derived from
the 1991–2020 period for each (station, month, decade) combination.

Missing days (NaN) are not treated as 0 mm.  The accumulated sum uses
nansum (observed days only), but if the entire decade window is NaN that
record is omitted from the output.  PRCD-Rel is also omitted when the
climatological mean for that decade is 0 mm (avoids division by zero in
very dry climates).

The four mng_indicators rows (short_name = "PRCD", "PRCD-Abs", "PRCD-Rel",
"PRCD-Cat") must exist and be enabled in the database before this calculator
is invoked.
"""

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from typing import Any, Dict, List, Optional, Tuple

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

# Short names of the four indicators required in mng_indicators
_SUB_INDICATORS = ("PRCD", "PRCD-Abs", "PRCD-Rel", "PRCD-Cat")

# Climatological norm period (fixed, independent of ETL date range)
_NORM_START = 1991
_NORM_END = 2020


def _is_leap(year: int) -> bool:
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def _decade_windows(year: int) -> List[Tuple[int, int, date, date]]:
    """
    Return the 36 decade windows for the given year as a list of
    (month, dec_num, start_date, end_date) tuples.

    dec_num 1 → days  1–10
    dec_num 2 → days 11–20
    dec_num 3 → days 21–last day of month
    """
    windows: List[Tuple[int, int, date, date]] = []
    for month in range(1, 13):
        windows.append((month, 1, date(year, month, 1), date(year, month, 10)))
        windows.append((month, 2, date(year, month, 11), date(year, month, 20)))
        # D3 ends on the last calendar day of the month
        if month == 12:
            end3 = date(year, 12, 31)
        else:
            end3 = date(year, month + 1, 1) - timedelta(days=1)
        windows.append((month, 3, date(year, month, 21), end3))
    return windows


class PRCDCalculator(BaseIndicatorCalculator):
    """
    Calculator for the PRCD decadal accumulated precipitation indicator group.

    INDICATOR_CODE = "PRCD" matches the short_name used in
    MngCountryIndicator.criteria to activate this calculator.
    """

    INDICATOR_CODE = "PRCD"
    SUPPORTED_TEMPORALITIES = ["annual"]
    SECONDARY_CODES = ["PRCD-Abs", "PRCD-Rel", "PRCD-Cat"]

    def __init__(
        self,
        indicator_config: Dict[str, Any],
        start_date: str,
        end_date: str,
        country_code: str,
    ) -> None:
        super().__init__(indicator_config, start_date, end_date, country_code)
        self._max_workers = int(os.getenv("MAX_PARALLEL_WORKERS", 4))
        # {short_name: IndicatorRead}
        self._indicator_meta: Dict[str, Any] = {}
        # {loc_id: {(month, dec_num): (mean, p33, p67)}}
        self._norm: Dict[int, Dict[Tuple[int, int], Tuple[float, float, float]]] = {}
        self._resolve_sub_indicators()

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def _resolve_sub_indicators(self) -> None:
        """Load the four PRCD indicator rows from mng_indicators by short_name."""
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
                "Create them before running the PRCD calculator."
            )

        info(
            "PRCD sub-indicators resolved from DB",
            component="prcd_calculator",
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
            "Fetching precipitation data for PRCD",
            component="prcd_calculator",
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
            warning("No precipitation data returned", component="prcd_calculator")
            return False

        info(
            "Years with data available for PRCD",
            component="prcd_calculator",
            years_available=sorted(yearly_data.keys()),
            target_years_missing=[y for y in target_years if y not in yearly_data],
        )

        self._build_norm(yearly_data, norm_years)

        all_records: List[ClimateHistoricalIndicatorCreate] = []

        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            futures = {
                executor.submit(self._process_year, year, yearly_data.get(year)): year
                for year in target_years
            }
            for future in as_completed(futures):
                year = futures[future]
                try:
                    year_records = future.result()
                    if year_records:
                        all_records.extend(year_records)
                        info(
                            "PRCD year summary",
                            component="prcd_calculator",
                            year=year,
                            records=len(year_records),
                        )
                except Exception as exc:
                    warning(
                        "PRCD year processing error",
                        component="prcd_calculator",
                        year=year,
                        error=str(exc),
                    )

        return self._save_results_to_db(all_records)

    # ------------------------------------------------------------------
    # Norm computation
    # ------------------------------------------------------------------

    def _build_norm(
        self,
        yearly_data: Dict[int, pd.DataFrame],
        norm_years: List[int],
    ) -> None:
        """
        Compute per-station, per-decade climatological statistics over 1991–2020:
        mean, percentile 33, and percentile 67 of P10(d).
        """
        # {loc_id: {(month, dec_num): [p10 values across norm years]}}
        accum: Dict[int, Dict[Tuple[int, int], List[float]]] = {}

        for year in norm_years:
            df = yearly_data.get(year)
            if df is None or df.empty:
                continue
            for loc_id, group in df.groupby("location_id"):
                loc_id = int(loc_id)
                series = self._to_julian_series(group, year)
                for month, dec_num, start_dt, end_dt in _decade_windows(year):
                    start_jday = start_dt.timetuple().tm_yday
                    end_jday = end_dt.timetuple().tm_yday
                    window = series.loc[start_jday:end_jday].to_numpy(dtype=float)
                    if np.all(np.isnan(window)):
                        continue
                    p10 = float(np.nansum(window))
                    accum.setdefault(loc_id, {}).setdefault(
                        (month, dec_num), []
                    ).append(p10)

        self._norm = {}
        for loc_id, decade_data in accum.items():
            self._norm[loc_id] = {}
            for dec_key, values in decade_data.items():
                arr = np.array(values, dtype=float)
                mean = float(np.mean(arr))
                p33 = float(np.percentile(arr, 33))
                p67 = float(np.percentile(arr, 67))
                self._norm[loc_id][dec_key] = (mean, p33, p67)

        norm_years_available = [y for y in norm_years if yearly_data.get(y) is not None]
        if len(norm_years_available) < 10:
            warning(
                "PRCD norm built from very few years — anomaly and category values "
                "will be unreliable. Load the full 1991–2020 baseline.",
                component="prcd_calculator",
                norm_years_available=norm_years_available,
            )

        info(
            "PRCD 1991-2020 norm computed",
            component="prcd_calculator",
            stations_with_norm=len(self._norm),
        )

    # ------------------------------------------------------------------
    # Per-year calculation
    # ------------------------------------------------------------------

    def _process_year(
        self,
        year: int,
        df: Optional[pd.DataFrame],
    ) -> Optional[List[ClimateHistoricalIndicatorCreate]]:
        """
        Compute PRCD, PRCD-Abs, PRCD-Rel, and PRCD-Cat for every station and
        every decade in one year.  Returns a flat list of ORM create objects
        ready for bulk_create, or None if there is no data for the year.
        """
        if df is None or df.empty:
            warning(
                "No data for year — skipping",
                component="prcd_calculator",
                year=year,
            )
            return None

        prcd_id = self._indicator_meta["PRCD"].id
        abs_id = self._indicator_meta["PRCD-Abs"].id
        rel_id = self._indicator_meta["PRCD-Rel"].id
        cat_id = self._indicator_meta["PRCD-Cat"].id

        records: List[ClimateHistoricalIndicatorCreate] = []
        windows = _decade_windows(year)

        for loc_id, group in df.groupby("location_id"):
            loc_id = int(loc_id)
            series = self._to_julian_series(group, year)
            station_norm = self._norm.get(loc_id, {})

            for month, dec_num, start_dt, end_dt in windows:
                start_jday = start_dt.timetuple().tm_yday
                end_jday = end_dt.timetuple().tm_yday
                window = series.loc[start_jday:end_jday].to_numpy(dtype=float)

                # Skip decade entirely if no observed data
                if np.all(np.isnan(window)):
                    continue

                p10 = float(np.nansum(window))

                # PRCD — raw decadal accumulation
                records.append(
                    ClimateHistoricalIndicatorCreate(
                        indicator_id=prcd_id,
                        location_id=loc_id,
                        value=p10,
                        period=Period.DECADAL,
                        start_date=start_dt,
                        end_date=end_dt,
                    )
                )

                # Anomalies and category require climatological norm
                norm_stats = station_norm.get((month, dec_num))
                if norm_stats is None:
                    continue

                mean, p33, p67 = norm_stats

                # PRCD-Abs
                records.append(
                    ClimateHistoricalIndicatorCreate(
                        indicator_id=abs_id,
                        location_id=loc_id,
                        value=p10 - mean,
                        period=Period.DECADAL,
                        start_date=start_dt,
                        end_date=end_dt,
                    )
                )

                # PRCD-Rel — omit when climatological mean is 0 (e.g. dry months)
                if mean != 0.0:
                    records.append(
                        ClimateHistoricalIndicatorCreate(
                            indicator_id=rel_id,
                            location_id=loc_id,
                            value=(p10 - mean) / mean * 100.0,
                            period=Period.DECADAL,
                            start_date=start_dt,
                            end_date=end_dt,
                        )
                    )

                # PRCD-Cat
                if p10 < p33:
                    cat = 1
                elif p10 <= p67:
                    cat = 2
                else:
                    cat = 3

                records.append(
                    ClimateHistoricalIndicatorCreate(
                        indicator_id=cat_id,
                        location_id=loc_id,
                        value=float(cat),
                        period=Period.DECADAL,
                        start_date=start_dt,
                        end_date=end_dt,
                    )
                )

        return records if records else None

    # ------------------------------------------------------------------
    # Core helpers
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

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _save_results_to_db(
        self,
        records: Any,
    ) -> bool:
        if not records:
            warning("No PRCD records to save", component="prcd_calculator")
            return True

        svc = ClimateHistoricalIndicatorService()
        saved = svc.bulk_create(records)

        info(
            "PRCD results saved to DB",
            component="prcd_calculator",
            saved=saved,
            total=len(records),
        )
        return bool(saved == len(records))
