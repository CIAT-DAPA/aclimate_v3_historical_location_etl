from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ...tools.logging_manager import error, info, warning


class BaseIndicatorCalculator(ABC):
    """
    Abstract base class for all point (station-level) climate indicator calculators.

    Each indicator calculator must implement this interface and define:
    - INDICATOR_CODE: Short name of the indicator (e.g., "TXX")
    - SUPPORTED_TEMPORALITIES: List of supported time periods (e.g., ["annual"])
    - calculate_* methods for each supported temporality
    - _save_results_to_db() to persist results via ORM
    """

    # Must be overridden in subclasses
    INDICATOR_CODE: str = ""
    SUPPORTED_TEMPORALITIES: List[str] = []

    # Additional short_names whose DB entries are produced by this same calculator.
    # These codes will also be registered in CalculatorLoader so the processor
    # can route them here.  The calculator is still only instantiated/run ONCE.
    SECONDARY_CODES: List[str] = []

    def __init__(
        self,
        indicator_config: Dict[str, Any],
        start_date: str,
        end_date: str,
        country_code: str,
    ):
        """
        Initialize the calculator.

        Args:
            indicator_config: Full indicator configuration dict from database (id, name,
                              short_name, unit, temporality, country_config, etc.)
            start_date: Start date in YYYY-MM format
            end_date: End date in YYYY-MM format
            country_code: ISO2 country code (e.g., "hn"), stored as lowercase
        """
        self.config = indicator_config
        self.start_date = start_date
        self.end_date = end_date
        self.country_code = country_code.lower()

        # Extract indicator details from config
        self.indicator_id: Optional[int] = indicator_config.get("id")
        self.indicator_name: str = indicator_config.get("name", "Unknown")
        self.short_name: str = indicator_config.get("short_name", "UNKNOWN")
        self.temporality: str = indicator_config.get("temporality", "annual")
        self.unit: str = indicator_config.get("unit", "")
        self.country_config: Dict[str, Any] = indicator_config.get("country_config", {})

        self._validate_required_attributes()

        info(
            "Indicator calculator initialized",
            component="indicator_calculation",
            indicator_code=self.INDICATOR_CODE,
            indicator_name=self.indicator_name,
            temporality=self.temporality,
            country_code=self.country_code,
        )

    def calculate(self) -> bool:
        """
        Main calculation method that routes to the appropriate temporality method.

        Returns:
            bool: True if calculation was successful, False otherwise
        """
        try:
            info(
                "Starting indicator calculation",
                component="indicator_calculation",
                indicator_code=self.INDICATOR_CODE,
                temporality=self.temporality,
            )

            if self.temporality not in self.SUPPORTED_TEMPORALITIES:
                error(
                    "Unsupported temporality for indicator",
                    component="indicator_calculation",
                    indicator_code=self.INDICATOR_CODE,
                    temporality=self.temporality,
                    supported=self.SUPPORTED_TEMPORALITIES,
                )
                return False

            method_name = f"calculate_{self.temporality}"
            if not hasattr(self, method_name):
                error(
                    "Calculation method not found",
                    component="indicator_calculation",
                    indicator_code=self.INDICATOR_CODE,
                    method_name=method_name,
                )
                return False

            method = getattr(self, method_name)
            result: bool = bool(method())

            if result:
                info(
                    "Indicator calculation completed successfully",
                    component="indicator_calculation",
                    indicator_code=self.INDICATOR_CODE,
                    temporality=self.temporality,
                )
            else:
                warning(
                    "Indicator calculation returned failure",
                    component="indicator_calculation",
                    indicator_code=self.INDICATOR_CODE,
                    temporality=self.temporality,
                )

            return result

        except Exception as e:
            error(
                "Exception during indicator calculation",
                component="indicator_calculation",
                indicator_code=self.INDICATOR_CODE,
                error=str(e),
            )
            return False

    def _validate_required_attributes(self) -> None:
        """Validate that required class attributes are defined in the subclass."""
        if not self.INDICATOR_CODE:
            raise ValueError(
                f"INDICATOR_CODE must be defined in {self.__class__.__name__}"
            )
        if not self.SUPPORTED_TEMPORALITIES:
            raise ValueError(
                f"SUPPORTED_TEMPORALITIES must be defined in {self.__class__.__name__}"
            )

    @abstractmethod
    def calculate_annual(self) -> bool:
        """Calculate annual indicator values and persist results via _save_results_to_db."""
        pass

    def calculate_monthly(self) -> bool:
        """Calculate monthly indicator values (optional override)."""
        warning(
            "Monthly calculation not implemented",
            component="indicator_calculation",
            indicator_code=self.INDICATOR_CODE,
        )
        return False

    def calculate_daily(self) -> bool:
        """Calculate daily indicator values (optional override)."""
        warning(
            "Daily calculation not implemented",
            component="indicator_calculation",
            indicator_code=self.INDICATOR_CODE,
        )
        return False

    def calculate_seasonal(self) -> bool:
        """Calculate seasonal indicator values (optional override)."""
        warning(
            "Seasonal calculation not implemented",
            component="indicator_calculation",
            indicator_code=self.INDICATOR_CODE,
        )
        return False

    @abstractmethod
    def _save_results_to_db(self, results: Dict[str, Any]) -> bool:
        """
        Persist calculation results to the database via ORM.

        Args:
            results: Calculator-specific results dictionary. Typically structured as
                     {year: {location_id: value}} for annual indicators.

        Returns:
            bool: True if all records were saved successfully, False if any failed.
        """
        pass
