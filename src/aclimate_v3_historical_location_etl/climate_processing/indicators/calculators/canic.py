"""
CANIC — Canícula: Inicio, Duración e Intensidad.

A single calculation process produces three outputs saved to
climate_historical_indicator for each (year, station):

  - CANIC     : first julian day j*_c in the July–August window (Jul 1 – Aug 31)
                where both canícula-onset conditions hold simultaneously:
                  (1) sum(P_d, j..j+9) < 0.75 · P̄_10,n(d_j)
                      (10-day precip below 75 % of the decadal climatological norm)
                  (2) dSSC(j) >= 5
                      (at least 5 consecutive dry days starting from j)

  - CANIC-Dur : duration  D_c = j_end − j* + 1  [days]
                j_end is the last canícula day, defined as the day before the
                first calendar-decade start D > j* where BOTH decade D and the
                immediately following decade satisfy P10 >= 0.75 · P̄_10,n.

  - CANIC-Int : intensity  I_c = (1 − P_real / P_norm) × 100  [%]
                P_real = sum(P_d^(i),       i = j* … j_end)
                P_norm = sum(P̄_d,n^(i),    i = j* … j_end)

                Intensity categories (WMO-No. 1246):
                  I_c < 25 %            → Leve
                  25 % ≤ I_c < 50 %     → Moderada
                  I_c ≥ 50 %            → Severa

Climatological norms (P̄_10,n and P̄_d,n) are derived from the 1991–2020 period.
Missing precipitation days are treated as 0 mm.

The three mng_indicators rows (short_name = "CANIC", "CANIC-Dur", "CANIC-Int")
must exist and be enabled in the database before this calculator is invoked.
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

# Short names of the three indicators required in mng_indicators
_SUB_INDICATORS = ("CANIC", "CANIC-Dur", "CANIC-Int")

# Climatological norm period (fixed, independent of ETL date range)
_NORM_START = 1991
_NORM_END = 2020

# Dry-day threshold (mm) — WMO standard
_DRY_THRESHOLD = 1.0

# Minimum consecutive dry days required for canícula onset
_MIN_DSSC = 5

# Fraction of decadal norm used in both onset and recovery conditions
_NORM_FRACTION = 0.75


def _is_leap(year: int) -> bool:
    return (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0)


def _jul_1_julian(year: int) -> int:
    """Return the 1-based julian day of July 1 for the given year."""
    return date(year, 7, 1).timetuple().tm_yday


def _aug_31_julian(year: int) -> int:
    """Return the 1-based julian day of August 31 for the given year."""
    return date(year, 8, 31).timetuple().tm_yday


class CANICCalculator(BaseIndicatorCalculator):
    """
    Calculator for the CANIC canícula indicator group.

    INDICATOR_CODE = "CANIC" matches the short_name used in
    MngCountryIndicator.criteria to activate this calculator.
    """

    INDICATOR_CODE = "CANIC"
    SUPPORTED_TEMPORALITIES = ["annual"]
    SECONDARY_CODES = ["CANIC-Dur", "CANIC-Int"]

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
        # {loc_id: {(month, dec_num): mean_10day_sum}} — decadal precipitation norm
        self._decade_norm: Dict[int, Dict[Tuple[int, int], float]] = {}
        # {loc_id: {(month, day_of_month): mean_daily_value}} — daily precipitation norm
        self._daily_norm: Dict[int, Dict[Tuple[int, int], float]] = {}
        # {loc_id: {year: jstar}} — IELL onset loaded optionally from DB
        self._iell_onset: Dict[int, Dict[int, int]] = {}
        self._resolve_sub_indicators()

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def _resolve_sub_indicators(self) -> None:
        """Load the three CANIC indicator rows from mng_indicators by short_name."""
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
                "Create them before running the CANIC calculator."
            )

        info(
            "CANIC sub-indicators resolved from DB",
            component="canic_calculator",
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
            "Fetching precipitation data for CANIC",
            component="canic_calculator",
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
            warning("No precipitation data returned", component="canic_calculator")
            return False

        info(
            "Years with data available for CANIC",
            component="canic_calculator",
            years_available=sorted(yearly_data.keys()),
            target_years_missing=[y for y in target_years if y not in yearly_data],
        )

        self._build_norm(yearly_data, norm_years)

        # Optionally load IELL onset data to validate that j* falls after the
        # rainy season has begun.  If IELL has not been calculated yet, or the
        # indicator is not present in mng_indicators, this step is skipped
        # silently and no validation is performed.
        self._load_iell_onset(target_years)

        results: Dict[str, Dict[int, Dict[int, float]]] = {
            "CANIC": {},
            "CANIC-Dur": {},
            "CANIC-Int": {},
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
                        results["CANIC"][year] = year_result["CANIC"]
                        results["CANIC-Dur"][year] = year_result["CANIC-Dur"]
                        results["CANIC-Int"][year] = year_result["CANIC-Int"]
                        info(
                            "CANIC year summary",
                            component="canic_calculator",
                            year=year,
                            stations_onset=len(year_result["CANIC"]),
                            stations_duration=len(year_result["CANIC-Dur"]),
                            stations_intensity=len(year_result["CANIC-Int"]),
                        )
                except Exception as exc:
                    warning(
                        "CANIC year processing error",
                        component="canic_calculator",
                        year=year,
                        error=str(exc),
                    )

        return self._save_results_to_db(results)

    # ------------------------------------------------------------------
    # IELL onset loader (optional validation)
    # ------------------------------------------------------------------

    def _load_iell_onset(self, target_years: List[int]) -> None:
        """
        Attempt to load IELL (rainy-season onset) records from the database
        for the target years.  Builds self._iell_onset as:
            {loc_id: {year: julian_day_of_rainy_onset}}

        This is entirely optional: if the IELL indicator does not exist in
        mng_indicators, or no records have been stored yet, the method logs a
        warning and leaves self._iell_onset empty.  When empty, no rainy-onset
        validation is performed in _process_year.
        """
        svc_ind = MngIndicatorService()
        iell_rows = svc_ind.get_by_short_name("IELL")
        if not iell_rows:
            warning(
                "IELL indicator not found in mng_indicators — "
                "rainy-season onset validation will be skipped for CANIC.",
                component="canic_calculator",
            )
            return

        iell_indicator_id: int = iell_rows[0].id
        svc_chi = ClimateHistoricalIndicatorService()
        records = svc_chi.get_by_indicator_id(iell_indicator_id)

        if not records:
            warning(
                "No IELL records found in climate_historical_indicator — "
                "rainy-season onset validation will be skipped for CANIC.",
                component="canic_calculator",
            )
            return

        target_year_set = set(target_years)
        loaded = 0
        for rec in records:
            rec_year = rec.start_date.year
            if rec_year not in target_year_set:
                continue
            loc_id = int(rec.location_id)
            self._iell_onset.setdefault(loc_id, {})[rec_year] = int(rec.value)
            loaded += 1

        info(
            "IELL onset data loaded for CANIC validation",
            component="canic_calculator",
            records_loaded=loaded,
            stations=len(self._iell_onset),
        )

    # ------------------------------------------------------------------
    # Norm computation
    # ------------------------------------------------------------------

    def _build_norm(
        self,
        yearly_data: Dict[int, pd.DataFrame],
        norm_years: List[int],
    ) -> None:
        """
        Compute per-station climatological norms over 1991–2020:

          _decade_norm[loc_id][(month, dec_num)] = mean 10-day precipitation
              sum starting from day 1, 11, or 21 of the given month.

          _daily_norm[loc_id][(month, day_of_month)] = mean daily precipitation
              for that calendar day, used as the per-day norm in the intensity
              formula.
        """
        norm_frames: List[pd.DataFrame] = []
        for year in norm_years:
            df = yearly_data.get(year)
            if df is not None and not df.empty:
                norm_frames.append(df)

        if not norm_frames:
            warning(
                "No norm-period data available for CANIC", component="canic_calculator"
            )
            return

        norm_years_available = [y for y in norm_years if yearly_data.get(y) is not None]
        if len(norm_years_available) < 10:
            warning(
                "CANIC norm built from very few years — results may be unreliable. "
                "The onset condition p10 < 0.75·norm is based on an average of only "
                f"{len(norm_years_available)} year(s); with <10 years the condition "
                "tends to be satisfied by at most one year per pair, causing other "
                "target years to produce no results. Load the full 1991–2020 baseline "
                "for reliable output.",
                component="canic_calculator",
                norm_years_available=norm_years_available,
            )

        all_data = pd.concat(norm_frames, ignore_index=True)
        all_data["date"] = pd.to_datetime(all_data["date"])
        all_data["value"] = all_data["value"].clip(lower=0).fillna(0.0)
        all_data["month"] = all_data["date"].dt.month
        all_data["day"] = all_data["date"].dt.day
        all_data["year_val"] = all_data["date"].dt.year
        all_data["julian"] = all_data["date"].dt.dayofyear

        # --- Daily norms ---
        # Aggregate multiple records per station-day then average over years.
        daily_grouped = (
            all_data.groupby(["location_id", "year_val", "month", "day"])["value"]
            .sum()
            .reset_index()
            .groupby(["location_id", "month", "day"])["value"]
            .mean()
        )
        self._daily_norm = {}
        for (loc_id, month, day), val in daily_grouped.items():
            self._daily_norm.setdefault(int(loc_id), {})[(int(month), int(day))] = (
                float(val)
            )

        # --- Decade norms ---
        # For each (station, year), build the julian series and sum the 10 days
        # starting from day 1, 11, or 21 of every month.
        decade_acc: Dict[int, Dict[Tuple[int, int], List[float]]] = {}

        for (loc_id, year_val), group in all_data.groupby(["location_id", "year_val"]):
            loc_id = int(loc_id)
            year_val = int(year_val)
            series_data = group.groupby("julian")["value"].sum()
            total_days = 366 if _is_leap(year_val) else 365
            values = series_data.reindex(range(1, total_days + 1)).to_numpy(dtype=float)

            for month in range(1, 13):
                for dec_num in (1, 2, 3):
                    day_start = (dec_num - 1) * 10 + 1
                    try:
                        j0 = (
                            date(year_val, month, day_start).timetuple().tm_yday - 1
                        )  # 0-based
                    except ValueError:
                        continue
                    window = values[j0 : j0 + 10]
                    if len(window) == 10:
                        decade_acc.setdefault(loc_id, {}).setdefault(
                            (month, dec_num), []
                        ).append(float(np.nansum(window)))

        self._decade_norm = {
            loc_id: {k: float(np.mean(v)) for k, v in decades.items()}
            for loc_id, decades in decade_acc.items()
        }

        info(
            "CANIC norms computed (1991–2020)",
            component="canic_calculator",
            stations=len(self._decade_norm),
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
        Compute CANIC, CANIC-Dur, and CANIC-Int for every station in one year.

        Returns a dict of {sub_indicator -> {loc_id -> value}}, or None if df
        is empty.
        """
        if df is None or df.empty:
            warning(
                "No data for year — skipping",
                component="canic_calculator",
                year=year,
            )
            return None

        canic_vals: Dict[int, float] = {}
        dur_vals: Dict[int, float] = {}
        int_vals: Dict[int, float] = {}

        for loc_id, group in df.groupby("location_id"):
            loc_id = int(loc_id)
            series = self._to_julian_series(group, year)
            decade_norm = self._decade_norm.get(loc_id, {})
            daily_norm = self._daily_norm.get(loc_id, {})

            if not decade_norm:
                warning(
                    "No decade norm for station — skipping",
                    component="canic_calculator",
                    year=year,
                    loc_id=loc_id,
                )
                continue

            jstar = self._find_jstar(series, year, decade_norm)
            if jstar is None:
                warning(
                    "No canícula onset found in Jul–Aug window — station skipped",
                    component="canic_calculator",
                    year=year,
                    loc_id=loc_id,
                )
                continue

            # Optional rainy-season onset validation: if IELL data is available
            # for this station and year, require that j* falls AFTER the rainy
            # season has begun.  Canícula cannot precede the rainy season.
            iell_year = self._iell_onset.get(loc_id, {}).get(year)
            if iell_year is not None and jstar <= iell_year:
                warning(
                    "CANIC j* is on or before rainy-season onset (IELL) — "
                    "likely a pre-season false positive; station skipped",
                    component="canic_calculator",
                    year=year,
                    loc_id=loc_id,
                    jstar=jstar,
                    iell_onset=iell_year,
                )
                continue

            jend = self._find_jend(series, year, jstar, decade_norm)
            if jend is None:
                warning(
                    "Canícula onset found but no recovery detected before August 21 — station skipped",
                    component="canic_calculator",
                    year=year,
                    loc_id=loc_id,
                    jstar=jstar,
                )
                continue

            canic_vals[loc_id] = float(jstar)
            dur_vals[loc_id] = float(jend - jstar + 1)

            intensity = self._compute_intensity(series, year, jstar, jend, daily_norm)
            if intensity is not None:
                int_vals[loc_id] = intensity

        return {
            "CANIC": canic_vals,
            "CANIC-Dur": dur_vals,
            "CANIC-Int": int_vals,
        }

    # ------------------------------------------------------------------
    # Core algorithm helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_julian_series(group: pd.DataFrame, year: int) -> pd.Series:
        """
        Convert a station's daily rows to a Series indexed by julian day (1-based).
        Negative precipitation values are clipped to 0. Missing days remain NaN.
        """
        g = group.copy()
        g["date"] = pd.to_datetime(g["date"])
        g["julian"] = g["date"].dt.dayofyear
        g["value"] = g["value"].clip(lower=0)
        series = g.groupby("julian")["value"].sum()
        total_days = 366 if _is_leap(year) else 365
        return series.reindex(range(1, total_days + 1))

    @classmethod
    def _find_jstar(
        cls,
        series: pd.Series,
        year: int,
        decade_norm: Dict[Tuple[int, int], float],
    ) -> Optional[int]:
        """
        Return the first 1-based julian day j* in [Jul 1, Aug 31] where:
          (1) sum(P[j..j+9]) < 0.75 · P̄_10,n(decade containing j)
          (2) dSSC(j)         >= 5  (consecutive dry days from j)

        NaN values are treated as 0 mm (both for the 10-day sum and as dry days).
        Days whose decade has a zero or missing norm are skipped.
        Returns None if no qualifying day is found in the search window.
        """
        search_start = _jul_1_julian(year) - 1  # 0-based index
        search_end = _aug_31_julian(year) - 1  # 0-based index (inclusive)
        values = series.to_numpy(dtype=float)
        n = len(values)

        # Need at least 10 days of lookahead
        max_start = min(search_end, n - 10)

        for i in range(search_start, max_start + 1):
            p10 = float(np.nansum(values[i : i + 10]))
            dec_key = cls._get_decade_key(i + 1, year)
            norm = decade_norm.get(dec_key, 0.0)
            if norm <= 0.0:
                continue  # cannot evaluate condition without a valid norm

            if p10 >= _NORM_FRACTION * norm:
                continue  # condition (1) not satisfied

            # Condition (1) holds; check consecutive dry days
            if cls._dssc(values, i) >= _MIN_DSSC:
                return i + 1  # 1-based julian day

        return None

    @classmethod
    def _find_jend(
        cls,
        series: pd.Series,
        year: int,
        jstar: int,
        decade_norm: Dict[Tuple[int, int], float],
    ) -> Optional[int]:
        """
        Return the last 1-based julian day of the canícula (j_end).

        Searches day by day within the July–August window, starting from
        jstar + 10 (the day immediately after the onset's 10-day window).
        The search upper bound is August 21 — the last position where a
        10-day window fits entirely within July–August.

        j_end is the first day j where:
            sum(P[j..j+9]) >= 0.75 · P̄_10,n(decade of j)

        The canícula period is [jstar, j_end] inclusive.

        Returns None if no recovery is found within the July–August window.
        """
        values = series.to_numpy(dtype=float)
        n = len(values)

        # 0-based: first candidate is 10 days after onset
        search_start = jstar + 9  # 0-based index → julian day jstar+10 (1-based)
        # For early onsets the upper bound is Aug 21 (last position where the
        # 10-day window fits within July–August without extending past Aug 31).
        # For late onsets (onset > ~Aug 11) search_start exceeds Aug 21, so we
        # extend the bound to search_start itself — capped at Aug 31.  The
        # 10-day window is allowed to spill into September because `values`
        # covers the full year (matching R's rollsum-on-full-year behaviour).
        aug_31_0 = _aug_31_julian(year) - 1  # 0-based index of Aug 31
        search_end = min(max(aug_31_0 - 10, search_start), aug_31_0)

        for i in range(search_start, search_end + 1):
            if i + 10 > n:
                break

            p10 = float(np.nansum(values[i : i + 10]))
            dec_key = cls._get_decade_key(i + 1, year)
            norm = decade_norm.get(dec_key, 0.0)
            if norm <= 0.0:
                continue

            if p10 >= _NORM_FRACTION * norm:
                return i + 1  # 1-based julian day (recovery day, inclusive end)

        return None

    @staticmethod
    def _dssc(values: np.ndarray, i: int) -> int:
        """
        Count consecutive dry days (observed P < _DRY_THRESHOLD) from 0-based
        index i onwards.  A missing observation (NaN) is not a dry day and
        immediately breaks the sequence.
        """
        count = 0
        n = len(values)
        while i + count < n:
            v = values[i + count]
            if np.isnan(v):
                break
            elif v < _DRY_THRESHOLD:
                count += 1
            else:
                break
        return count

    @staticmethod
    def _get_decade_key(julian_day: int, year: int) -> Tuple[int, int]:
        """Return (month, dec_num) for a 1-based julian day."""
        d = date(year, 1, 1) + timedelta(days=julian_day - 1)
        if d.day <= 10:
            dec_num = 1
        elif d.day <= 20:
            dec_num = 2
        else:
            dec_num = 3
        return (d.month, dec_num)

    @staticmethod
    def _compute_intensity(
        series: pd.Series,
        year: int,
        jstar: int,
        jend: int,
        daily_norm: Dict[Tuple[int, int], float],
    ) -> Optional[float]:
        """
        I_c = (1 − P_real / P_norm) × 100  [%]

        P_real = sum of observed daily precip from jstar to jend (inclusive, 1-based).
        P_norm = sum of climatological daily norms for the same period.

        Returns None when P_norm is zero (cannot divide).
        """
        values = series.to_numpy(dtype=float)
        actual_sum = float(np.nansum(values[jstar - 1 : jend]))

        base_date = date(year, 1, 1)
        norm_sum = 0.0
        for offset in range(jend - jstar + 1):
            d = base_date + timedelta(days=jstar - 1 + offset)
            norm_sum += daily_norm.get((d.month, d.day), 0.0)

        if norm_sum <= 0.0:
            return None

        return (1.0 - actual_sum / norm_sum) * 100.0

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
            "CANIC results saved to DB",
            component="canic_calculator",
            saved=saved,
            total=len(records),
        )
        return bool(saved == len(records))
