import importlib.util
import inspect
import sys
import types
from pathlib import Path
from typing import Dict, Optional, Type

from ...tools.logging_manager import error, info, warning
from .base_calculator import BaseIndicatorCalculator


class CalculatorLoader:
    """
    Automatically discovers and loads indicator calculators from the calculators/ directory.

    This class implements the Module Discovery pattern to automatically find and register
    all indicator calculators without requiring manual registration.  Adding a new
    calculator is as simple as dropping a .py file into the calculators/ directory.
    """

    _calculators: Dict[str, Type[BaseIndicatorCalculator]] = {}
    _loaded: bool = False

    @classmethod
    def load_all(cls) -> None:
        """
        Auto-discover and load all indicator calculators from the calculators/ directory.

        Scans the directory for .py files, imports them dynamically, and registers
        any class that inherits from BaseIndicatorCalculator and passes validation.
        """
        if cls._loaded:
            return

        info(
            "Starting auto-discovery of indicator calculators",
            component="calculator_loader",
        )

        try:
            calculators_dir = Path(__file__).parent / "calculators"

            if not calculators_dir.exists():
                warning(
                    "Calculators directory does not exist",
                    component="calculator_loader",
                    calculators_dir=str(calculators_dir),
                )
                cls._loaded = True
                return

            # Only load regular .py files; skip __init__.py and private files
            calculator_files = [
                f for f in calculators_dir.glob("*.py") if not f.name.startswith("_")
            ]

            info(
                f"Found {len(calculator_files)} potential calculator files",
                component="calculator_loader",
                files=[f.name for f in calculator_files],
            )

            loaded_count = 0
            failed_count = 0

            for py_file in calculator_files:
                try:
                    cls._load_calculator_from_file(py_file)
                    loaded_count += 1
                except Exception as e:
                    failed_count += 1
                    error(
                        f"Failed to load calculator file: {py_file.name}",
                        component="calculator_loader",
                        file=py_file.name,
                        error=str(e),
                    )

            info(
                f"Calculator discovery completed: {loaded_count} loaded, {failed_count} failed",
                component="calculator_loader",
                total_loaded=loaded_count,
                failed=failed_count,
                registered_indicators=list(cls._calculators.keys()),
            )

            cls._loaded = True

        except Exception as e:
            error(
                f"Failed to load calculators: {str(e)}",
                component="calculator_loader",
                error=str(e),
            )
            cls._loaded = True  # Prevent infinite retry

    @classmethod
    def _load_calculator_from_file(cls, py_file: Path) -> None:
        """Load calculator classes from a specific Python file."""
        module_name = py_file.stem

        try:
            # Derive the full dotted module name so that relative imports inside
            # each calculator file resolve correctly.
            loader_pkg = __name__.rsplit(".", 1)[0]
            calculators_pkg = f"{loader_pkg}.calculators"
            full_module_name = f"{calculators_pkg}.{module_name}"

            # Ensure the calculators sub-package is present in sys.modules
            # before loading a file from it; Python needs this to resolve
            # relative imports (e.g. "from ..base_calculator import ...").
            if calculators_pkg not in sys.modules:
                calculators_init = py_file.parent / "__init__.py"
                if calculators_init.exists():
                    calc_spec = importlib.util.spec_from_file_location(
                        calculators_pkg, calculators_init
                    )
                    if calc_spec is None or calc_spec.loader is None:
                        return
                    calc_mod = importlib.util.module_from_spec(calc_spec)
                    calc_mod.__path__ = [str(py_file.parent)]
                    calc_mod.__package__ = calculators_pkg
                    sys.modules[calculators_pkg] = calc_mod
                    calc_spec.loader.exec_module(calc_mod)
                else:
                    calc_mod = types.ModuleType(calculators_pkg)
                    calc_mod.__path__ = [str(py_file.parent)]
                    calc_mod.__package__ = calculators_pkg
                    sys.modules[calculators_pkg] = calc_mod

            spec = importlib.util.spec_from_file_location(full_module_name, py_file)
            if spec is None or spec.loader is None:
                warning(
                    f"Cannot create module spec for {py_file.name}",
                    component="calculator_loader",
                    file=py_file.name,
                )
                return

            module = importlib.util.module_from_spec(spec)
            sys.modules[full_module_name] = module
            spec.loader.exec_module(module)

            calculators_found = 0

            for name, obj in inspect.getmembers(module, inspect.isclass):
                if not name.endswith("Calculator"):
                    continue
                if name == "BaseIndicatorCalculator":
                    continue
                if not getattr(obj, "INDICATOR_CODE", None):
                    continue
                if not getattr(obj, "SUPPORTED_TEMPORALITIES", None):
                    continue

                if not cls._validate_calculator_class(obj):
                    warning(
                        f"Skipping invalid calculator class {name}",
                        component="calculator_loader",
                        class_name=name,
                        file=py_file.name,
                    )
                    continue

                indicator_code = obj.INDICATOR_CODE.upper()

                if indicator_code in cls._calculators:
                    warning(
                        f"Duplicate INDICATOR_CODE '{indicator_code}' — skipping {name}",
                        component="calculator_loader",
                        indicator_code=indicator_code,
                        existing=cls._calculators[indicator_code].__name__,
                        duplicate=name,
                    )
                    continue

                cls._calculators[indicator_code] = obj
                calculators_found += 1

                info(
                    f"Registered calculator '{name}' for indicator '{indicator_code}'",
                    component="calculator_loader",
                    indicator_code=indicator_code,
                    class_name=name,
                    file=py_file.name,
                )

                for secondary_code in getattr(obj, "SECONDARY_CODES", None) or []:
                    secondary_upper = secondary_code.upper()
                    cls._calculators[secondary_upper] = obj
                    info(
                        f"Registered '{name}' as secondary handler for '{secondary_upper}'",
                        component="calculator_loader",
                        primary_code=indicator_code,
                        secondary_code=secondary_upper,
                    )

            if calculators_found == 0:
                warning(
                    f"No valid calculator classes found in {py_file.name}",
                    component="calculator_loader",
                    file=py_file.name,
                )

        except Exception as e:
            error(
                f"Error loading module {module_name}",
                component="calculator_loader",
                module_name=module_name,
                error=str(e),
            )
            raise

    @classmethod
    def _validate_calculator_class(cls, calculator_class: Type) -> bool:
        """
        Validate that a calculator class meets all requirements.

        Args:
            calculator_class: The class to validate

        Returns:
            bool: True if the class is valid, False otherwise
        """
        try:
            if not getattr(calculator_class, "INDICATOR_CODE", None):
                warning(
                    f"Class {calculator_class.__name__} missing INDICATOR_CODE",
                    component="calculator_loader",
                )
                return False

            if not getattr(calculator_class, "SUPPORTED_TEMPORALITIES", None):
                warning(
                    f"Class {calculator_class.__name__} missing or empty SUPPORTED_TEMPORALITIES",
                    component="calculator_loader",
                )
                return False

            for temporality in calculator_class.SUPPORTED_TEMPORALITIES:
                method_name = f"calculate_{temporality}"
                if not hasattr(calculator_class, method_name):
                    warning(
                        f"Class {calculator_class.__name__} missing method '{method_name}'",
                        component="calculator_loader",
                        class_name=calculator_class.__name__,
                        method_name=method_name,
                    )
                    return False

            return True

        except Exception as e:
            error(
                f"Error validating calculator class {calculator_class.__name__}",
                component="calculator_loader",
                error=str(e),
            )
            return False

    @classmethod
    def get_calculator(
        cls, indicator_code: str
    ) -> Optional[Type[BaseIndicatorCalculator]]:
        """
        Get a calculator class for the specified indicator code.

        Args:
            indicator_code: Indicator short name / code (e.g., "TXX")

        Returns:
            The calculator class if found, None otherwise
        """
        if not cls._loaded:
            cls.load_all()

        calculator_class = cls._calculators.get(indicator_code.upper())

        if calculator_class:
            info(
                f"Found calculator for indicator {indicator_code}",
                component="calculator_loader",
                indicator_code=indicator_code,
                calculator_class=calculator_class.__name__,
            )
        else:
            warning(
                f"No calculator found for indicator {indicator_code}",
                component="calculator_loader",
                indicator_code=indicator_code,
                available_indicators=list(cls._calculators.keys()),
            )

        return calculator_class

    @classmethod
    def get_available_indicators(cls) -> Dict[str, Type[BaseIndicatorCalculator]]:
        """
        Return all registered indicator calculators.

        Returns:
            Dict mapping INDICATOR_CODE strings to calculator classes
        """
        if not cls._loaded:
            cls.load_all()
        return cls._calculators.copy()

    @classmethod
    def reload(cls) -> None:
        """Force reload of all calculators (useful in development / testing)."""
        info(
            "Forcing reload of all calculators",
            component="calculator_loader",
        )
        cls._calculators.clear()
        cls._loaded = False
        cls.load_all()

    @classmethod
    def is_indicator_supported(cls, indicator_code: str, temporality: str) -> bool:
        """
        Check whether an indicator supports a specific temporality.

        Args:
            indicator_code: Indicator short name (e.g., "TXX")
            temporality: Temporality to check (e.g., "annual")

        Returns:
            bool: True if supported
        """
        calculator_class = cls.get_calculator(indicator_code)
        if not calculator_class:
            return False
        return temporality in calculator_class.SUPPORTED_TEMPORALITIES
