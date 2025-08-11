# ge_runner.py
# Executes Great Expectations validations on ingested data and logs results.
# ==============================================================================

import os
import json
import logging
import pandas as pd
from typing import Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

def run_data_validation(
    data: pd.DataFrame,
    data_source: str,
    table_name: str,
    environment: str,
    pipeline_level: str
) -> Dict[str, Any]:
    """
    Run data validation checks using Great Expectations.

    Returns:
        Dictionary with validation results
    """
    try:
        logger.info(f"Starting data validation for {data_source}.{table_name}")

        validation_results = {
            "success": True,
            "timestamp": datetime.utcnow().isoformat(),
            "data_source": data_source,
            "table_name": table_name,
            "environment": environment,
            "pipeline_level": pipeline_level,
            "total_rows": len(data),
            "total_columns": len(data.columns),
            "null_checks": {},
            "duplicate_checks": {},
        }
        # Run basic data quality checks
        run_null_checks(data, validation_results)
        run_duplicate_checks(data, validation_results)
        run_basic_stats_checks(data, validation_results)

        # Run custom expectations if configured
        expectations_config = load_expectations_config(data_source, table_name)
        if expectations_config:
            run_custom_expectations(data, expectations_config, validation_results)

        # Calculate overall quality score
        validation_results["data_quality_score"] = calculate_quality_score(validation_results)

        logger.info(f"Validation completed - Quality Score: {validation_results['data_quality_score']:.2f}")

        return validation_results

    except Exception as e:
        logger.error(f"Data validation failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat(),
            "data_source": data_source,
            "table_name": table_name
        }
def run_null_checks(data: pd.DataFrame, results: Dict[str, Any]):
    """Run null value checks and update results."""
    null_counts = data.isnull().sum()
    null_percentages = (null_counts / len(data) * 100).round(2)

    for column in data.columns:
        results["null_checks"][column] = {
            "null_count": int(null_counts[column]),
            "null_percentage": float(null_percentages[column]),
            "has_nulls": null_counts[column] > 0,
            "total_rows": len(data)
        }

def run_duplicate_checks(data: pd.DataFrame, results: Dict[str, Any]):
    """Run duplicate row checks and update results."""
    total_duplicates = data.duplicated().sum()
    results["duplicate_checks"] = {
        "total_duplicate_rows": int(total_duplicates),
        "duplicate_percentage": float((total_duplicates / len(data) * 100).round(2)),
        "has_duplicates": total_duplicates > 0,
        "unique_rows": int(len(data) - total_duplicates),
        "total_rows": len(data)
    }
def run_basic_stats_checks(data: pd.DataFrame, results: Dict[str, Any]):
    """Run basic statistical checks and update results."""
    results["basic_stats"] = {
        "memory_usage_mb": float(data.memory_usage(deep=True).sum() / 1024 / 1024),
        "column_types": data.dtypes.astype(str).to_dict(),
        "numeric_columns": list(data.select_dtypes(include=['number']).columns),
        "string_columns": list(data.select_dtypes(include=['object']).columns),
        "datetime_columns": list(data.select_dtypes(include=['datetime']).columns)
    }

def load_expectations_config(data_source: str, table_name: str) -> Dict[str, Any]:
    """Load Great Expectations configuration for the source/table."""
    try:
        config_file = f"/app/gx/{data_source}_expectations.json"
        if os.path.exists(config_file):
            with open(config_file, 'r') as f:
                config = json.load(f)
            return config.get(table_name, {})
        return {}
    except Exception as e:
        logger.warning(f"Could not load expectations config: {e}")
        return {}

def run_custom_expectations(data: pd.DataFrame, config: Dict[str, Any], results: Dict[str, Any]):
    """Run custom expectations based on configuration."""

    custom_checks = []

    # 1. Check required columns (schema validation)
    if "expect_columns_to_match_set" in config:
        column_config = config["expect_columns_to_match_set"]
        expected_columns = set(column_config["column_set"])
        actual_columns = set(data.columns)
        exact_match = column_config.get("exact_match", False)

        if exact_match:
            success = expected_columns == actual_columns
            details = f"Expected exact columns: {sorted(expected_columns)}, Got: {sorted(actual_columns)}"
        else:
            success = expected_columns.issubset(actual_columns)
            missing = expected_columns - actual_columns
            details = f"Missing columns: {sorted(missing)}" if missing else "All expected columns present"

        custom_checks.append({
            "check_type": "expect_columns_to_match_set",
            "column": None,
            "success": success,
            "details": details
        })

    # 2. Check for null values in required columns
    if "expect_column_values_to_not_be_null" in config:
        for column in config["expect_column_values_to_not_be_null"]:
            if column in data.columns:
                null_count = data[column].isnull().sum()
                success = null_count == 0
                details = f"Column '{column}' has {null_count} null values" if not success else f"Column '{column}' has no null values"

                custom_checks.append({
                    "check_type": "expect_column_values_to_not_be_null",
                    "column": column,
                    "success": success,
                    "details": details
                })

    # 3. Check unique columns
    if "expect_column_values_to_be_unique" in config:
        for column in config["expect_column_values_to_be_unique"]:
            if column in data.columns:
                duplicate_count = data[column].duplicated().sum()
                success = duplicate_count == 0
                details = f"Column '{column}' has {duplicate_count} duplicates" if not success else f"Column '{column}' has no duplicates"

                custom_checks.append({
                    "check_type": "expect_column_values_to_be_unique",
                    "column": column,
                    "success": success,
                    "details": details
                })
    # 4. Check data types
    if "expect_column_values_to_be_of_type" in config:
        for column, expected_type in config["expect_column_values_to_be_of_type"].items():
            if column in data.columns:
                actual_type = str(data[column].dtype)
                success = actual_type == expected_type or _is_compatible_type(actual_type, expected_type)
                details = f"Column '{column}' type: expected {expected_type}, got {actual_type}"

                custom_checks.append({
                    "check_type": "expect_column_values_to_be_of_type",
                    "column": column,
                    "success": success,
                    "details": details
                })

    # 5. Check string formats (regex patterns)
    if "expect_column_values_to_match_string_format" in config:
        for column, pattern in config["expect_column_values_to_match_string_format"].items():
            if column in data.columns:
                import re
                # Convert to string and check pattern
                string_data = data[column].astype(str)
                matches = string_data.str.match(pattern, na=False)
            match_count = matches.sum()
            total_count = len(data[column].dropna())
            success_rate = (match_count / total_count * 100) if total_count > 0 else 0
            success = success_rate >= 95 # 95% threshold for string format compliance

            details = f"Column '{column}' format compliance: {success_rate:.1f}% ({match_count}/{total_count})"

            custom_checks.append({
                "check_type": "expect_column_values_to_match_string_format",
                "column": column,
                "success": success,
                "details": details
            })

    # 6. Check compound column uniqueness
    if "expect_compound_columns_to_be_unique" in config:
        for column_group in config["expect_compound_columns_to_be_unique"]:
            if all(col in data.columns for col in column_group):
                duplicate_count = data[column_group].duplicated().sum()
                success = duplicate_count == 0
                column_str = "+".join(column_group)
                details = f"Compound columns '{column_str}' have {duplicate_count} duplicates" if not success else f"Compound columns '{column_str}' are unique"

                custom_checks.append({
                    "check_type": "expect_compound_columns_to_be_unique",
                    "column": column_str, # Using column_str for the 'column' field
                    "success": success,
                    "details": details
                })
    # 7. Check value sets (categorical validation)
    if "expect_column_values_to_be_in_set" in config:
        for column, allowed_values in config["expect_column_values_to_be_in_set"].items():
            if column in data.columns:
                unique_values = set(data[column].dropna().unique())
                allowed_set = set(allowed_values)
                invalid_values = unique_values - allowed_set
                success = len(invalid_values) == 0
                details = f"Column '{column}' invalid values: {sorted(invalid_values)}" if not success else f"Column '{column}' has only allowed values"

                custom_checks.append({
                    "check_type": "expect_column_values_to_be_in_set",
                    "column": column,
                    "success": success,
                    "details": details
                })

    # 8. Check numeric ranges
    if "expect_column_values_to_be_between" in config:
        for column, range_config in config["expect_column_values_to_be_between"].items():
            if column in data.columns and data[column].dtype in ['int64', 'float64']:
                min_val = range_config.get("min_value")
                max_val = range_config.get("max_value")

                out_of_range = 0
                if min_val is not None:
                    out_of_range += (data[column] < min_val).sum()
                if max_val is not None:
                    out_of_range += (data[column] > max_val).sum()

                success = out_of_range == 0
                details = f"Column '{column}' has {out_of_range} values outside range [{min_val}, {max_val}]" if not success else f"Column '{column}' values are within range [{min_val}, {max_val}]"

                custom_checks.append({
                    "check_type": "expect_column_values_to_be_between",
                    "column": column,
                    "success": success,
                    "details": details
                })

    results["custom_checks"] = custom_checks

    def _is_compatible_type(actual_type: str, expected_type: str) -> bool:
        """Check if actual pandas dtype is compatible with expected type."""
        # ... (rest of the function's content, which is likely a dictionary mapping and logic)
        type_mappings = {
            'object': ['string', 'str'],
            'int64': ['int', 'integer', 'int64'],
            'float64': ['float', 'double', 'float64'],
            'bool': ['boolean', 'bool'],
            'datetime64[ns]': ['datetime', 'timestamp', 'datetime64[ns]']
        }

    for pandas_type, compatible_types in type_mappings.items():
        if actual_type == pandas_type and expected_type in compatible_types:
            return True

    return False

    def calculate_quality_score(validation_results: Dict[str, Any]) -> float:
        """Calculate overall data quality score (0-100)."""
        try:
            score = 100.0

            # Penalize for high null percentages
            null_penalty = 0
            for column, null_info in validation_results.get("null_checks", {}).items():
                null_penalty += null_info.get("null_percentage", 0) * 0.1

            # Penalize for duplicates
            duplicate_penalty = validation_results.get("duplicate_checks", {}).get("duplicate_percentage", 0) * 0.5

            # Penalize for failed custom checks
            failed_checks = sum(1 for check in validation_results.get("custom_checks", []) if not check.get("success"))
            custom_penalty = failed_checks * 5

            final_score = max(0, score - null_penalty - duplicate_penalty - custom_penalty)
            return round(final_score, 2)

        except Exception as e:
            logger.error(f"Quality score calculation failed: {e}")
            return 0.0
    