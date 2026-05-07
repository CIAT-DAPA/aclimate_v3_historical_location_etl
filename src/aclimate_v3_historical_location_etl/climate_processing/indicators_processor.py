"""
IndicatorsProcessor — orchestrates point-level climate indicator calculation.

Queries the database for all location_climate indicators configured for the
target country, then dispatches each one to the appropriate calculator via
CalculatorLoader (auto-discovery).
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from aclimate_v3_orm.services import (
    MngCountryIndicatorService,
    MngCountryService,
    MngIndicatorService,
)

from ..tools.logging_manager import error, info, warning
from .indicators import CalculatorLoader


class IndicatorsProcessor:
    """
    Main orchestrator for point (station-level) climate indicators.

    Usage::

        processor = IndicatorsProcessor(
            country="HONDURAS",
            start_date="2000-01",
            end_date="2020-12",
        )
        processor.process_all_indicators()
    """

    def __init__(
        self,
        country: str,
        start_date: str,
        end_date: str,
    ):
        """
        Initialize the processor.

        Args:
            country: Target country name in any case (stored as uppercase).
            start_date: Start date in YYYY-MM format.
            end_date: End date in YYYY-MM format.

        The country ISO2 code is resolved automatically from the database.
        """
        try:
            self.country = country.upper()
            self.start_date = start_date
            self.end_date = end_date
            self.country_code: Optional[str] = None  # resolved lazily from DB

            # ORM services
            self.country_indicator_service = MngCountryIndicatorService()
            self.indicator_service = MngIndicatorService()
            self.country_service = MngCountryService()

            # Runtime caches
            self.country_data: Optional[Dict[str, Any]] = None
            self.country_indicators: List[Dict[str, Any]] = []

            info(
                "IndicatorsProcessor initialized successfully",
                component="indicators_init",
                country=self.country,
                start_date=start_date,
                end_date=end_date,
            )

        except Exception as e:
            error(
                "Failed to initialize IndicatorsProcessor",
                component="indicators_init",
                country=country,
                error=str(e),
            )
            raise

    # ------------------------------------------------------------------
    # Data retrieval
    # ------------------------------------------------------------------

    def _get_country_data(self) -> Dict[str, Any]:
        """
        Fetch (and cache) country data from the database.

        Returns:
            Country record dict.

        Raises:
            ValueError: If the country is not found in the database.
        """
        try:
            if self.country_data is None:
                countries = self.country_service.get_by_name(self.country)
                if not countries:
                    raise ValueError(f"Country '{self.country}' not found in database")
                self.country_data = countries[0].model_dump()

                # Resolve iso2 from DB (field is 'iso2', stored as lowercase)
                self.country_code = self.country_data["iso2"].lower()

                info(
                    "Country data retrieved from database",
                    component="indicators_db",
                    country=self.country,
                    country_id=self.country_data.get("id"),
                    iso2=self.country_code,
                )

            return self.country_data

        except Exception as e:
            error(
                "Failed to retrieve country data",
                component="indicators_db",
                country=self.country,
                error=str(e),
            )
            raise

    def _get_country_indicators(self) -> List[Dict[str, Any]]:
        """
        Fetch (and cache) all location_climate indicators configured for the country.

        Returns:
            List of indicator config dicts, each enriched with indicator details
            (id, name, short_name, unit) and country_config (criteria).
        """
        try:
            if not self.country_indicators:
                country_data = self._get_country_data()
                country_id = country_data["id"]

                # Get all country-indicator associations
                country_indicator_links = self.country_indicator_service.get_by_country(
                    country_id
                )

                indicators = []
                for link in country_indicator_links:
                    # Filter: only location_climate (point) indicators
                    if not link.location_climate:
                        continue

                    # Retrieve indicator details by id
                    indicator = self.indicator_service.get_by_id(link.indicator_id)

                    if indicator is None:
                        warning(
                            "Indicator not found — skipping",
                            component="indicators_db",
                            indicator_id=link.indicator_id,
                        )
                        continue

                    # criteria JSON may carry a 'temporality' key;
                    # default to 'annual' when not specified.
                    criteria = link.criteria or {}
                    temporality = criteria.get("temporality", "annual")

                    # Compose config dict passed to calculators
                    indicator_config = {
                        "id": indicator.id,
                        "name": indicator.name,
                        "short_name": indicator.short_name,
                        "unit": indicator.unit,
                        "temporality": temporality,
                        "country_config": criteria,
                    }
                    indicators.append(indicator_config)

                self.country_indicators = indicators

                info(
                    "Country location_climate indicators retrieved",
                    component="indicators_db",
                    country=self.country,
                    indicator_count=len(self.country_indicators),
                    indicators=[i["short_name"] for i in self.country_indicators],
                )

            return self.country_indicators

        except Exception as e:
            error(
                "Failed to retrieve country indicators",
                component="indicators_db",
                country=self.country,
                error=str(e),
            )
            raise

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate_date_range(self) -> None:
        """
        Validate that start_date and end_date are in YYYY-MM format
        and that start_date <= end_date.

        Raises:
            ValueError: On invalid format or inverted range.
        """
        try:
            info(
                "Validating date range",
                component="indicators_validation",
                start_date=self.start_date,
                end_date=self.end_date,
            )
            start = datetime.strptime(self.start_date, "%Y-%m")
            end = datetime.strptime(self.end_date, "%Y-%m")
            if start > end:
                raise ValueError(
                    f"start_date '{self.start_date}' must be <= end_date '{self.end_date}'"
                )
        except ValueError as e:
            error(
                "Date range validation failed",
                component="indicators_validation",
                error=str(e),
            )
            raise

    # ------------------------------------------------------------------
    # Main processing pipeline
    # ------------------------------------------------------------------

    def process_all_indicators(self) -> bool:
        """
        Main entry point: validate dates, load indicators, and run each calculator.

        Returns:
            bool: True if every indicator processed without error; False otherwise.
        """
        try:
            self._validate_date_range()

            indicators = self._get_country_indicators()

            if not indicators:
                warning(
                    "No location_climate indicators configured for country",
                    component="indicators_processor",
                    country=self.country,
                )
                return True  # Not an error — nothing to do

            info(
                "Processing all location_climate indicators",
                component="indicators_processor",
                country=self.country,
                total=len(indicators),
            )

            overall_success = True
            # Track which calculator classes have already been run so that
            # secondary-code indicators (e.g. IELL-Anomalie, IELL-decade)
            # don't trigger a duplicate execution of the same calculator.
            executed_calculators: set = set()

            for indicator in indicators:
                short_name = indicator.get("short_name", "UNKNOWN")
                calculator_class = CalculatorLoader.get_calculator(short_name)

                if (
                    calculator_class is not None
                    and calculator_class in executed_calculators
                ):
                    info(
                        "Skipping secondary indicator — already handled by its primary calculator",
                        component="indicators_processor",
                        indicator=short_name,
                        calculator=calculator_class.__name__,
                    )
                    continue

                success = self._process_single_indicator(indicator)

                if calculator_class is not None:
                    executed_calculators.add(calculator_class)

                if not success:
                    overall_success = False
                    warning(
                        "Indicator processing failed",
                        component="indicators_processor",
                        indicator=short_name,
                    )

            info(
                "All indicators processed",
                component="indicators_processor",
                country=self.country,
                success=overall_success,
            )
            return overall_success

        except Exception as e:
            error(
                "Exception in process_all_indicators",
                component="indicators_processor",
                country=self.country,
                error=str(e),
            )
            return False

    def _process_single_indicator(self, indicator: Dict[str, Any]) -> bool:
        """
        Instantiate and run the calculator for a single indicator.

        Args:
            indicator: Config dict with at minimum keys:
                       id, name, short_name, unit, temporality, country_config

        Returns:
            bool: True if the calculator ran successfully.
        """
        short_name = indicator.get("short_name", "UNKNOWN")
        try:
            info(
                "Processing indicator",
                component="indicators_processor",
                indicator=short_name,
                temporality=indicator.get("temporality"),
            )

            calculator_class = CalculatorLoader.get_calculator(short_name)
            if calculator_class is None:
                warning(
                    "No calculator found for indicator — skipping",
                    component="indicators_processor",
                    indicator=short_name,
                )
                return False

            # Ensure country_code is resolved before instantiating calculators
            if self.country_code is None:
                self._get_country_data()

            calculator = calculator_class(
                indicator_config=indicator,
                start_date=self.start_date,
                end_date=self.end_date,
                country_code=self.country,
            )

            return calculator.calculate()

        except Exception as e:
            error(
                "Exception processing single indicator",
                component="indicators_processor",
                indicator=short_name,
                error=str(e),
            )
            return False

    # ------------------------------------------------------------------
    # Convenience accessors
    # ------------------------------------------------------------------

    def get_available_indicators(self) -> List[Dict[str, Any]]:
        """
        Return the list of location_climate indicators configured for the country.

        Returns:
            List of indicator config dicts.
        """
        try:
            return self._get_country_indicators()
        except Exception as e:
            error(
                "Failed to get available indicators",
                component="indicators_processor",
                error=str(e),
            )
            return []

    def get_indicator_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Find a specific indicator by its short_name (case-insensitive).

        Args:
            name: Indicator short_name to look up (e.g., "TXX")

        Returns:
            Indicator config dict or None if not found.
        """
        try:
            indicators = self._get_country_indicators()
            return next(
                (i for i in indicators if i["short_name"].upper() == name.upper()),
                None,
            )
        except Exception as e:
            error(
                "Failed to get indicator by name",
                component="indicators_processor",
                name=name,
                error=str(e),
            )
            return None
