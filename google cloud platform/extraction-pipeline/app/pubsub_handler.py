# ==============================================================================
# pubsub_handler.py
# Parses and processes Pub/Sub messages to trigger ingestion jobs.
# ==============================================================================

import base64
import json
import logging
from typing import Dict, Any
from datetime import datetime
from template_source.app.extract_api import extract_data
from ge_runner import run_data_validation
from bigquery_utils import load_to_bigquery, log_metadata

logger = logging.getLogger(__name__)

def handle_pubsub_message(envelope: Dict[str, Any], data_source: str) -> Dict[str, Any]:
    """
    Handle incoming Pub/Sub message and orchestrate the pipeline.

    Args:
        envelope: Pub/Sub message envelope
        data_source: Name of the data source (workday, ccure, etc.)

    Returns:
        Dictionary with success status and details
    """
    try:
        # Decode message
        message_data = decode_pubsub_message(envelope)
        logger.info(f"Processing {data_source} pipeline: {message_data}")

        # Extract required parameters
        table_name = message_data.get("table_name")
        environment = message_data.get("environment", "dev")
        pipeline_level = message_data.get("level", "bronze")

        if not table_name:
            raise ValueError("table_name is required in Pub/Sub message")

        # Step 1: Extract data
        logger.info(f"Starting data extraction for {data_source}.{table_name}")
        extraction_result = extract_data(
            data_source=data_source,
            table_name=table_name,
            environment=environment,
            **message_data
        )

        if not extraction_result.get("success"):
            return {
                "success": False,
                "error": f"Data extraction failed: {extraction_result.get('error')}"
            }

        data = extraction_result["data"]
        logger.info(f"Extracted {len(data)} rows")

        # Step 2: Run data validation
        logger.info(f"Starting data validation for {data_source}.{table_name}")
        validation_result = run_data_validation(
            data=data,
            data_source=data_source,
            table_name=table_name,
            environment=environment,
            pipeline_level=pipeline_level
        )

        # Step 3: Load to BigQuery
        logger.info(f"Loading data to BigQuery for {data_source}.{table_name}")
        load_result = load_to_bigquery(
            data=data,
            data_source=data_source,
            table_name=table_name,
            environment=environment,
            pipeline_level=pipeline_level,
            validation_results=validation_result
        )

        if not load_result.get("success"):
            return {
                "success": False,
                "error": f"BigQuery load failed: {load_result.get('error')}"
            }

        # Step 4: Log monitoring metadata
        logger.info(f"Logging metadata for {data_source}.{table_name}")
        log_metadata(
            data_source=data_source,
            table_name=table_name,
            environment=environment,
            pipeline_level=pipeline_level,
            validation_results=validation_result,
            load_results=load_result
        )

        return {
            "success": True,
            "message": f"Pipeline completed for {data_source}.{table_name}",
            "rows_processed": len(data),
            "validation_passed": validation_result.get("success", False),
            "data_quality_score": validation_result.get("data_quality_score", 0)
        }
        
    except Exception as e:
        logger.exception(f"Pipeline failed for {data_source}: {e}")
        return {"success": False, "error": str(e)}

def decode_pubsub_message(envelope: Dict[str, Any]) -> Dict[str, Any]:
    """Decode Pub/Sub message from envelope."""
    try:
        message = envelope.get("message", {})

        # Decode base64 data if present
        message_data = {}
        if "data" in message:
            data = base64.b64decode(message["data"]).decode("utf-8")
            message_data = json.loads(data)

        # Add attributes
        if "attributes" in message:
            message_data.update(message["attributes"])

        return message_data

    except Exception as e:
        logger.error(f"Failed to decode Pub/Sub message: {e}")
        raise ValueError(f"Invalid Pub/Sub message format: {e}")