"""
Data Validator for Historical Location ETL Pipeline.
Validates extracted data quality, completeness, and integrity.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from .tools.logging_manager import info, warning, error


class DataValidator:
    """
    Validates extracted climate data for quality, completeness, and integrity.
    """
    
    def __init__(self):
        """Initialize the data validator."""
        self.metadata_columns = ['location_id', 'location_name', 'latitude', 'longitude', 'date']
        self.required_columns = ['location_id', 'location_name', 'latitude', 'longitude', 'date']
        info("Data validator initialized", component="data_validator")
    
    def validate_extracted_data(self, df: pd.DataFrame, 
                              start_date: datetime, 
                              end_date: datetime,
                              expected_locations: List[int],
                              clean_data: bool = True) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Comprehensive validation and cleaning of extracted data.
        
        Args:
            df: DataFrame with extracted climate data
            start_date: Expected start date
            end_date: Expected end date
            expected_locations: List of expected location IDs
            clean_data: Whether to perform data cleaning
            
        Returns:
            Tuple of (cleaned_dataframe, validation_results_dict)
        """
        try:
            info("Starting data validation",
                 component="data_validator",
                 total_records=len(df),
                 date_range=f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                 expected_locations=len(expected_locations))
            
            cleaned_df = df.copy()
            errors = []
            warnings = []
            
            # 1. Structure validation
            structure_errors = self._validate_structure(cleaned_df)
            errors.extend(structure_errors)
            
            # 2. Data completeness validation
            completeness_errors, completeness_warnings = self._validate_completeness(
                cleaned_df, start_date, end_date, expected_locations
            )
            errors.extend(completeness_errors)
            warnings.extend(completeness_warnings)
            
            # 3. Data quality validation
            quality_errors, quality_warnings = self._validate_quality(cleaned_df)
            errors.extend(quality_errors)
            warnings.extend(quality_warnings)
            
            # 4. Clean data if requested
            cleaning_actions = []
            if clean_data and not errors:  # Only clean if no critical errors
                cleaned_df, cleaning_actions = self._clean_data(cleaned_df)
            
            # Build results
            validation_results = {
                'is_valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings,
                'statistics': {
                    'total_records': len(df),
                    'total_columns': len(df.columns),
                    'has_valid_structure': len(structure_errors) == 0
                },
                'cleaning_actions': cleaning_actions
            }
            
            # Log summary
            status = "PASSED" if validation_results['is_valid'] else "FAILED"
            info(f"Data validation {status}",
                 component="data_validator",
                 errors_count=len(errors),
                 warnings_count=len(warnings))
            
            return cleaned_df, validation_results
            
        except Exception as e:
            error("Validation failed", component="data_validator", error=str(e))
            return df, {
                'is_valid': False,
                'errors': [f"Validation process failed: {str(e)}"],
                'warnings': [],
                'statistics': {},
                'cleaning_actions': []
            }
    
    def _validate_structure(self, df: pd.DataFrame) -> List[str]:
        """Validate DataFrame structure."""
        errors = []
        
        if df.empty:
            errors.append("DataFrame is empty")
            return errors
        
        missing_columns = [col for col in self.required_columns if col not in df.columns]
        if missing_columns:
            errors.append(f"Missing required columns: {missing_columns}")
        
        return errors
    
    def _validate_completeness(self, df: pd.DataFrame, start_date: datetime, 
                             end_date: datetime, expected_locations: List[int]) -> Tuple[List[str], List[str]]:
        """Validate data completeness."""
        errors = []
        warnings = []
        
        # Date completeness
        if 'date' in df.columns:
            expected_dates = pd.date_range(start=start_date, end=end_date, freq='D')
            actual_dates = pd.to_datetime(df['date']).dt.date.unique()
            date_coverage = len(actual_dates) / len(expected_dates) * 100
            
            if date_coverage < 90:
                errors.append(f"Insufficient date coverage: {date_coverage:.1f}%")
            elif date_coverage < 100:
                warnings.append(f"Incomplete date coverage: {date_coverage:.1f}%")
        
        # Location completeness
        if 'location_id' in df.columns:
            actual_locations = set(df['location_id'].unique())
            expected_locations_set = set(expected_locations)
            location_coverage = len(actual_locations & expected_locations_set) / len(expected_locations_set) * 100
            
            if location_coverage < 90:
                errors.append(f"Insufficient location coverage: {location_coverage:.1f}%")
            elif location_coverage < 100:
                warnings.append(f"Incomplete location coverage: {location_coverage:.1f}%")
        
        return errors, warnings
    
    def _validate_quality(self, df: pd.DataFrame) -> Tuple[List[str], List[str]]:
        """Validate data quality."""
        errors = []
        warnings = []
        
        data_columns = [col for col in df.columns if col not in self.metadata_columns]
        
        # Overall completeness check
        for col in data_columns:
            completeness = (df[col].notna().sum() / len(df)) * 100
            
            if completeness == 0:
                errors.append(f"CRITICAL: Variable '{col}' is completely empty across all locations and dates")
            elif completeness < 30:
                errors.append(f"CRITICAL: Variable '{col}' has very low completeness: {completeness:.1f}% (interpolation largely failed)")
            elif completeness < 70:
                warnings.append(f"Variable '{col}' has low completeness: {completeness:.1f}%")
            elif completeness < 95:
                warnings.append(f"Variable '{col}' has minor missing data: {completeness:.1f}%")
        
        # Location-specific validation with flexible approach
        location_errors, location_warnings = self._validate_locations_quality_flexible(df, data_columns)
        errors.extend(location_errors)
        warnings.extend(location_warnings)
        
        # Outlier detection
        outlier_warnings = self._detect_outliers(df, data_columns)
        warnings.extend(outlier_warnings)
        
        return errors, warnings
    
    def _validate_locations_quality_flexible(self, df: pd.DataFrame, data_columns: List[str]) -> Tuple[List[str], List[str]]:
        """Validate quality per location with flexible criteria for mixed variable availability."""
        errors = []
        warnings = []
        
        # Define critical variables (at least one must be present)
        critical_variables = ['prec', 'tmax', 'tmin', 'temperature', 'precipitation', 'rad']
        
        for location_id in df['location_id'].unique():
            location_data = df[df['location_id'] == location_id]
            location_name = location_data['location_name'].iloc[0] if not location_data.empty else f"Location {location_id}"
            
            # Check which variables have any data and which are completely missing
            available_vars = []
            missing_vars = []
            critical_vars_available = []
            
            for col in data_columns:
                valid_count = location_data[col].notna().sum()
                total_count = len(location_data)
                
                if valid_count > 0:
                    available_vars.append(col)
                    if col in critical_variables:
                        critical_vars_available.append(col)
                    
                    # Check for partial missing data (less than 95% complete)
                    completeness = (valid_count / total_count) * 100
                    if completeness < 95:
                        warnings.append(f"Location '{location_name}' (ID: {location_id}) variable '{col}' has {completeness:.1f}% completeness ({valid_count}/{total_count} records)")
                else:
                    # Completely missing variable
                    missing_vars.append(col)
            
            # Error if no critical variables are available
            if not critical_vars_available:
                errors.append(f"Location '{location_name}' (ID: {location_id}) has NO critical climate variables available")
            
            # Error for completely missing variables (interpolation failed)
            if missing_vars:
                errors.append(f"Location '{location_name}' (ID: {location_id}) has completely missing variables (interpolation failed): {', '.join(missing_vars)}")
            
            # Warning for limited variable availability but at least some data exists
            elif len(available_vars) == 1:
                warnings.append(f"Location '{location_name}' (ID: {location_id}) has only 1 variable available: {', '.join(available_vars)}")
            elif len(available_vars) < len(data_columns) * 0.5:  # Less than 50% of expected variables
                warnings.append(f"Location '{location_name}' (ID: {location_id}) has limited variables ({len(available_vars)}/{len(data_columns)}): {', '.join(available_vars)}")
        
        return errors, warnings
    
    def _detect_outliers(self, df: pd.DataFrame, data_columns: List[str]) -> List[str]:
        """Detect outliers using IQR method."""
        warnings = []
        
        for col in data_columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                valid_data = df[col].dropna()
                if len(valid_data) > 0:
                    Q1, Q3 = valid_data.quantile([0.25, 0.75])
                    IQR = Q3 - Q1
                    outliers = valid_data[(valid_data < Q1 - 1.5 * IQR) | (valid_data > Q3 + 1.5 * IQR)]
                    outlier_pct = (len(outliers) / len(valid_data)) * 100
                    
                    if outlier_pct > 5:
                        warnings.append(f"High outlier percentage for {col}: {outlier_pct:.1f}%")
        
        return warnings
    
    def _clean_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """Clean data (placeholder for future implementation)."""
        actions = [
            "Data cleaning: Strategy pending implementation",
            "NaN handling: Strategy pending implementation",
            "Outlier handling: Strategy pending implementation"
        ]
        
        info("Data cleaning - PLACEHOLDER", 
             component="data_validator",
             note="TODO: Implement cleaning strategies")
        
        return df, actions
    
    def generate_validation_report(self, validation_results: Dict[str, Any]) -> str:
        """Generate a detailed validation report."""
        try:
            lines = [
                "=" * 60,
                "DATA VALIDATION REPORT",
                "=" * 60,
                f"Overall Status: {'PASSED' if validation_results['is_valid'] else 'FAILED'}",
                f"Errors: {len(validation_results['errors'])}",
                f"Warnings: {len(validation_results['warnings'])}",
                ""
            ]
            
            # Statistics
            stats = validation_results.get('statistics', {})
            if stats:
                lines.append("BASIC STATISTICS:")
                for key, value in stats.items():
                    if not isinstance(value, (list, dict)):
                        lines.append(f"  {key}: {value}")
                lines.append("")
            
            # Errors
            if validation_results['errors']:
                lines.append("ERRORS:")
                for error in validation_results['errors']:
                    lines.append(f"  [ERROR] {error}")
                lines.append("")
            
            # Warnings
            if validation_results['warnings']:
                lines.append("WARNINGS:")
                for warning in validation_results['warnings']:
                    lines.append(f"  [WARNING] {warning}")
                lines.append("")
            
            # Cleaning actions
            if validation_results.get('cleaning_actions'):
                lines.append("DATA CLEANING ACTIONS:")
                for action in validation_results['cleaning_actions']:
                    lines.append(f"  [CLEAN] {action}")
                lines.append("")
            
            lines.append("=" * 60)
            return "\n".join(lines)
            
        except Exception as e:
            error("Failed to generate validation report", component="data_validator", error=str(e))
            return f"Error generating validation report: {str(e)}"
