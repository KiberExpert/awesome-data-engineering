# =============================================================================
# bigquery_utils.py
# Loads processed and validated data into BigQuery tables.
# =============================================================================

import os
import logging
import pandas as pd
from typing import Dict, Any
from datetime import datetime
from google.cloud import bigquery

logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_MAPPING = {
    "dev": "data_dev",
    "staging": "data_staging",
    "prod": "data_prod"
}
METADATA_DATASET = "data_quality_metadata"

def load_to_bigquery(data: pd.DataFrame, data_source: str, table_name: str,
             environment: str, pipeline_level: str, validation_results: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load data to BigQuery with metadata columns.

    Args:
        data: DataFrame to load
        data_source: Source system name
        table_name: Table name
        environment: Environment (dev, staging, prod)
        pipeline_level: Pipeline level (raw, cleaned, etc.)
        validation_results: Results from data validation

    Returns:
        Dictionary with load results
    """
    try:
        client = bigquery.Client(project=PROJECT_ID)

        # Determine target table
        dataset_name = DATASET_MAPPING.get(environment, "data_dev")
        full_table_name = f"{data_source}_{table_name}_{pipeline_level}"
        table_id = f"{PROJECT_ID}.{dataset_name}.{full_table_name}"

        # Add metadata columns
        data_with_metadata = data.copy()
        data_with_metadata["_ingestion_timestamp"] = datetime.utcnow()
        data_with_metadata["_data_source"] = data_source
        data_with_metadata["_environment"] = environment
        data_with_metadata["_pipeline_level"] = pipeline_level
        data_with_metadata["_data_quality_score"] = validation_results.get("data_quality_score", 0)
        data_with_metadata["_validation_passed"] = validation_results.get("success", False)

        # Configure load job
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )

        logger.info(f"Loading {len(data_with_metadata)} rows to {table_id}")

        # Load data
        job = client.load_table_from_dataframe(data_with_metadata, table_id, job_config=job_config)
        job.result()  # Wait for completion

        # Get updated table info
        table = client.get_table(table_id)

        result = {
            "success": True,
            "table_id": table_id,
            "rows_loaded": table.num_rows,
            "bytes_loaded": table.num_bytes,
            "load_timestamp": datetime.utcnow().isoformat()
        }

        logger.info(f"Successfully loaded {table.num_rows} rows to {table_id}")
        return result

    except Exception as e:
        logger.error(f"BigQuery load failed: {e}")
        return {"success": False, "error": str(e)}

def log_metadata(data_source: str, table_name: str, environment: str, pipeline_level: str,
                 validation_results: Dict[str, Any], load_results: Dict[str, Any]):
    """Log all monitoring metadata to BigQuery tables."""
    try:
        log_pipeline_execution(data_source, table_name, environment, pipeline_level, validation_results, load_results)
        log_null_monitoring(data_source, table_name, environment, pipeline_level, validation_results)
        log_duplicate_monitoring(data_source, table_name, environment, pipeline_level, validation_results)
        log_custom_checks(data_source, table_name, environment, pipeline_level, validation_results)

        logger.info(f"Successfully logged metadata for {data_source}.{table_name}")

    except Exception as e:
        logger.error(f"Metadata logging failed: {e}")
        
def log_pipeline_execution(data_source: str, table_name: str, environment: str, pipeline_level: str,
                           validation_results: Dict[str, Any], load_results: Dict[str, Any]):
    """Log pipeline execution summary."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{METADATA_DATASET}.pipeline_executions"

    ensure_metadata_table_exists(client, table_id, get_pipeline_execution_schema())

    row = {
        "execution_timestamp": validation_results.get("timestamp"),
        "data_source": data_source,
        "table_name": table_name,
        "environment": environment,
        "pipeline_level": pipeline_level,
        "rows_processed": validation_results.get("total_rows", 0),
        "data_quality_score": validation_results.get("data_quality_score", 0),
        "validation_passed": validation_results.get("success", False),
        "load_success": load_results.get("success", False),
        "target_table": load_results.get("table_id"),
        "execution_duration_seconds": None # Could be calculated if timing is tracked
    }

    client.insert_rows_json(client.get_table(table_id), [row])
    
def log_null_monitoring(data_source: str, table_name: str, environment: str, pipeline_level: str,
                       validation_results: Dict[str, Any]):
    """Log null value monitoring results."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{METADATA_DATASET}.null_monitoring"

    ensure_metadata_table_exists(client, table_id, get_null_monitoring_schema())

    rows = []
    for column, null_info in validation_results.get("null_checks", {}).items():
        row = {
            "timestamp": validation_results.get("timestamp"),
            "data_source": data_source,
            "table_name": table_name,
            "environment": environment,
            "pipeline_level": pipeline_level,
            "column_name": column,
            "null_count": null_info.get("null_count", 0),
            "null_percentage": null_info.get("null_percentage", 0),
            "total_rows": null_info.get("total_rows", 0),
            "has_nulls": null_info.get("has_nulls", False)
        }
        rows.append(row)
        
    if rows:
        client.insert_rows_json(client.get_table(table_id), rows)
        
def log_duplicate_monitoring(data_source: str, table_name: str, environment: str, pipeline_level: str,
                             validation_results: Dict[str, Any]):
    """Log duplicate monitoring results."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{METADATA_DATASET}.duplicate_monitoring"

    ensure_metadata_table_exists(client, table_id, get_duplicate_monitoring_schema())

    duplicate_info = validation_results.get("duplicate_checks", {})
    row = {
        "timestamp": validation_results.get("timestamp"),
        "data_source": data_source,
        "table_name": table_name,
        "environment": environment,
        "pipeline_level": pipeline_level,
        "total_rows": duplicate_info.get("total_rows", 0),
        "duplicate_rows": duplicate_info.get("duplicate_rows", 0),
        "duplicate_percentage": duplicate_info.get("duplicate_percentage", 0),
        "unique_rows": duplicate_info.get("unique_rows", 0),
        "has_duplicates": duplicate_info.get("has_duplicates", False)
    }

    client.insert_rows_json(client.get_table(table_id), [row])
        
def log_custom_checks(data_source: str, table_name: str, environment: str, pipeline_level: str,
                      validation_results: Dict[str, Any]):
    """Log custom Great Expectations checks."""
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{METADATA_DATASET}.custom_checks"

    ensure_metadata_table_exists(client, table_id, get_custom_checks_schema())

    rows = []
    for check in validation_results.get("custom_checks", []):
        row = {
            "timestamp": validation_results.get("timestamp"),
            "data_source": data_source,
            "table_name": table_name,
            "environment": environment,
            "pipeline_level": pipeline_level,
            "check_type": check.get("check_type"),
            "column_name": check.get("column"),
            "success": check.get("success", False),
            "details": check.get("details", "")
        }
        rows.append(row)

    if rows:
        client.insert_rows_json(client.get_table(table_id), rows)
        
def ensure_metadata_table_exists(client: bigquery.Client, table_id: str, schema: list):
    """Ensure metadata table exists with proper schema."""
    try:
        client.get_table(table_id)
    except:
        # Table doesn't exist, create it
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table)

def get_pipeline_execution_schema():
    """Schema for pipeline execution tracking table."""
    return [
        bigquery.SchemaField("execution_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("environment", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pipeline_level", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("rows_processed", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("data_quality_score", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("validation_passed", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("load_success", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("target_table", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("execution_duration_seconds", "FLOAT", mode="NULLABLE")
    ]

def get_null_monitoring_schema():
    """Schema for null value monitoring table."""
    return [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("environment", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pipeline_level", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("column_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("null_count", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("null_percentage", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("total_rows", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("has_nulls", "BOOLEAN", mode="NULLABLE")
    ]
    
def get_duplicate_monitoring_schema():
    """Schema for duplicate monitoring table."""
    return [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("environment", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pipeline_level", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("total_rows", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("duplicate_rows", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("duplicate_percentage", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("unique_rows", "INTEGER", mode="NULLABLE"),
        bigquery.SchemaField("has_duplicates", "BOOLEAN", mode="NULLABLE")
    ]
    
def get_custom_checks_schema():
    """Schema for custom Great Expectations checks table."""
    return [
        bigquery.SchemaField("timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("data_source", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("table_name", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("environment", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("pipeline_level", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("check_type", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("column_name", "STRING", mode="NULLABLE"),
        bigquery.SchemaField("success", "BOOLEAN", mode="NULLABLE"),
        bigquery.SchemaField("details", "STRING", mode="NULLABLE")
    ]
