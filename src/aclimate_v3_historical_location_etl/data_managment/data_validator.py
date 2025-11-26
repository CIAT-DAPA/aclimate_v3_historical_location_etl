"""
Data Validator for Historical Location ETL Pipeline.
Validates extracted data quality, completeness, and integrity.
"""

from datetime import datetime
from typing import Any, Dict, List, Tuple

import pandas as pd

from ..tools.logging_manager import error, info


class DataValidator:
    """
    Validates extracted climate data for quality, completeness, and integrity.
    """

    def __init__(self) -> None:
        """Initialize the data validator."""
        self.metadata_columns = [
            "location_id",
            "location_name",
            "latitude",
            "longitude",
            "date",
        ]
        self.required_columns = [
            "location_id",
            "location_name",
            "latitude",
            "longitude",
            "date",
        ]
        info("Data validator initialized", component="data_validator")

    def validate_extracted_data(
        self,
        df: pd.DataFrame,
        start_date: datetime,
        end_date: datetime,
        expected_locations: List[int],
        clean_data: bool = True,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Simple validation of extracted data - only checks that data exists.

        Args:
            df: DataFrame with extracted climate data
            start_date: Expected start date (not used)
            end_date: Expected end date (not used)
            expected_locations: List of expected location IDs (not used)
            clean_data: Whether to perform data cleaning (not used)

        Returns:
            Tuple of (dataframe, validation_results_dict)
        """
        try:
            info(
                "Starting simple data validation",
                component="data_validator",
                total_records=len(df),
            )

            errors = []

            # Only validate basic structure and data existence
            structure_errors = self._validate_structure(df)
            errors.extend(structure_errors)

            data_errors = self._validate_data_exists(df)
            errors.extend(data_errors)

            # Build results
            validation_results = {
                "is_valid": len(errors) == 0,
                "errors": errors,
                "warnings": [],
                "statistics": {
                    "total_records": len(df),
                    "total_columns": len(df.columns),
                },
                "cleaning_actions": [],
            }

            # Log summary
            status = "PASSED" if validation_results["is_valid"] else "FAILED"
            info(
                f"Data validation {status}",
                component="data_validator",
                errors_count=len(errors),
            )

            return df, validation_results

        except Exception as e:
            error("Validation failed", component="data_validator", error=str(e))
            return df, {
                "is_valid": False,
                "errors": [f"Validation process failed: {str(e)}"],
                "warnings": [],
                "statistics": {},
                "cleaning_actions": [],
            }

    def _validate_structure(self, df: pd.DataFrame) -> List[str]:
        """Validate DataFrame structure."""
        errors = []

        if df.empty:
            errors.append("DataFrame is empty")
            return errors

        missing_columns = [
            col for col in self.required_columns if col not in df.columns
        ]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")

        return errors

    def _validate_data_exists(self, df: pd.DataFrame) -> List[str]:
        """Validate that data exists in the dataframe."""
        errors = []

        if df.empty:
            errors.append("DataFrame is empty - no data to validate")
            return errors

        data_columns = [col for col in df.columns if col not in self.metadata_columns]

        if not data_columns:
            errors.append("No data columns found - only metadata columns present")
            return errors

        # Check if there's any actual data in any column
        has_any_data = False
        for col in data_columns:
            if df[col].notna().any():
                has_any_data = True
                break

        if not has_any_data:
            errors.append("No actual data found - all data columns are empty")

        return errors

    def generate_validation_report(self, validation_results: Dict[str, Any]) -> str:
        """Generate a detailed validation report."""
        try:
            lines = [
                "=" * 60,
                "DATA VALIDATION REPORT",
                "=" * 60,
                f"Overall Status: "
                f"{'PASSED' if validation_results['is_valid'] else 'FAILED'}",
                f"Errors: {len(validation_results['errors'])}",
                f"Warnings: {len(validation_results['warnings'])}",
                "",
            ]

            # Statistics
            stats = validation_results.get("statistics", {})
            if stats:
                lines.append("BASIC STATISTICS:")
                for key, value in stats.items():
                    if not isinstance(value, (list, dict)):
                        lines.append(f"  {key}: {value}")
                lines.append("")

            # Errors
            if validation_results["errors"]:
                lines.append("ERRORS:")
                for error_msg in validation_results["errors"]:
                    lines.append(f"  [ERROR] {error_msg}")
                lines.append("")

            # Warnings
            if validation_results["warnings"]:
                lines.append("WARNINGS:")
                for warning_msg in validation_results["warnings"]:
                    lines.append(f"  [WARNING] {warning_msg}")
                lines.append("")

            # Cleaning actions
            if validation_results.get("cleaning_actions"):
                lines.append("DATA CLEANING ACTIONS:")
                for action in validation_results["cleaning_actions"]:
                    lines.append(f"  [CLEAN] {action}")
                lines.append("")

            lines.append("=" * 60)
            return "\n".join(lines)

        except Exception as e:
            error(
                "Failed to generate validation report",
                component="data_validator",
                error=str(e),
            )
            return f"Error generating validation report: {str(e)}"
