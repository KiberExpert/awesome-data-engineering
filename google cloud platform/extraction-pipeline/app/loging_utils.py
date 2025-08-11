# ==============================================================================
# # logging_utils.py - Structured logging configuration
# ==============================================================================

import os
import sys
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional
from google.cloud import logging as cloud_logging

def setup_logging():
    """
    Set up structured logging for Cloud Run environment.
    Logs will go to Cloud Logging and be visible in Google Cloud Console.
    """

    # Get config from .env
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    service_name = os.getenv("SERVICE_NAME", "service_name")
    data_source = os.getenv("DATA_SOURCE", "source_name")
    environment = os.getenv("ENVIRONMENT", "dev")

    # Configure root logger
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(message)s', # Handled in the custom formatter
        handlers=[
            StructuredLogHandler(service_name, data_source, environment)
        ]
    )

    # Set up Cloud Logging client (for GCP environments)
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("GCP_PROJECT_ID"):
        try:
            client = cloud_logging.Client()
            cloud_logging.setup_logging()
            logging.info("Cloud Logging configured successfully")
        except Exception as e:
            logging.warning(f"Could not configure Cloud Logging: {e}")

    # Suppress other loggers
    logging.getLogger("google").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)

    class StructuredLogHandler(logging.StreamHandler):
        """
        Custom log handler that outputs structured JSON logs.
        This format is automatically parsed by Cloud Logging.
        """
        def __init__(self, service_name: str, data_source: str, environment: str):
            super().__init__(sys.stdout)
            self.service_name = service_name
            self.data_source = data_source
            self.environment = environment

        def format(self, record: logging.LogRecord) -> str:
            """Format log record as structured JSON."""

            # BASE LOG ENTRY
            log_entry = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "severity": record.levelname,
                "message": record.getMessage(),
                "service": self.service_name,
                "data_source": self.data_source,
                "environment": self.environment,
                "logger": record.name,
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno
            }

            # Add exception info if present
            if record.exc_info:
                log_entry["exception"] = self.formatException(record.exc_info)
            # Add extra fields if present (from logger.info("msg", extra={"key": "value"}))
            if hasattr(record, "extra_fields"):
                log_entry.update(record.extra_fields)

            # Add trace information for Cloud Logging correlation
            trace_header = os.getenv("HTTP_X_CLOUD_TRACE_CONTEXT")
            if trace_header:
                trace_id = trace_header.split("/")[0]
                log_entry["logging.googleapis.com/trace"] = f"projects/{os.getenv('GCP_PROJECT_ID')}/traces/{trace_id}"

            return json.dumps(log_entry)

    def get_logger(name: str) -> logging.Logger:
        """
        Get a logger instance with the specified name.

        Args:
            name: Logger name (usually __name__)

        Returns:
            Configured logger instance
        """
        return logging.getLogger(name)

    def log_pipeline_start(logger: logging.Logger, data_source: str, table_name: str):
        """Log pipeline start with structured metadata."""
        logger.info(
            f"Pipeline started for {data_source}.{table_name}",
            extra={
                "extra_fields": {
                    "event_type": "pipeline_start",
                    "data_source": data_source,
                    "table_name": table_name,
                    "environment": environment
                }
            }
        )

    def log_pipeline_end(logger: logging.Logger, data_source: str, table_name: str,
                        environment: str, success: bool, duration_seconds: float = None,
                        rows_processed: int = None, data_quality_score: float = None):
        """Log pipeline completion with structured metadata."""

        extra_fields = {
            "event_type": "pipeline_end",
            "data_source": data_source,
            "table_name": table_name,
            "environment": environment,
            "success": success
        }
        if duration_seconds is not None:
            extra_fields["duration_seconds"] = duration_seconds
        if rows_processed is not None:
            extra_fields["rows_processed"] = rows_processed
        if data_quality_score is not None:
            extra_fields["data_quality_score"] = data_quality_score

        message = f"Pipeline {'completed' if success else 'failed'} for {data_source}.{table_name}"
        if rows_processed:
            message += f" - {rows_processed} rows processed"

        logger.info(message, extra={"extra_fields": extra_fields})

    def log_extraction_metrics(logger: logging.Logger, data_source: str, table_name: str,
                            rows_extracted: int, extraction_time_seconds: float,
                            source_metadata: Dict[str, Any] = None):
        """Log data extraction metrics."""

        extra_fields = {
            "event_type": "extraction_metrics",
            "data_source": data_source,
            "table_name": table_name,
            "rows_extracted": rows_extracted,
            "extraction_time_seconds": extraction_time_seconds
        }
        if source_metadata:
            extra_fields["source_metadata"] = source_metadata

        logger.info(
            f"Extracted {rows_extracted} rows from {data_source}.{table_name} in {extraction_time_seconds:.2f}s",
            extra={"extra_fields": extra_fields}
        )

    def log_validation_metrics(logger: logging.Logger, data_source: str, table_name: str,
                            validation_results: Dict[str, Any]):
        """Log data validation metrics."""

        extra_fields = {
            "event_type": "validation_metrics",
            "data_source": data_source,
            "table_name": table_name,
            "validation_success": validation_results.get("success", False),
            "data_quality_score": validation_results.get("data_quality_score", 0),
            "total_rows": validation_results.get("total_rows", 0),
            "null_columns": len([col for col, info in validation_results.get("null_checks", {}).items() if info.get("has_nulls")]),
            "duplicate_percentage": validation_results.get("duplicate_checks", {}).get("duplicate_percentage", 0)
        }

        logger.info(
            f"Validation completed for {data_source}.{table_name} - Score: {validation_results.get('data_quality_score')}",
            extra={"extra_fields": extra_fields}
        )

    def log_bigquery_load(logger: logging.Logger, data_source: str, table_name: str,
                        load_results: Dict[str, Any]):
        """Log BigQuery load metrics."""

        extra_fields = {
            "event_type": "bigquery_load",
            "data_source": data_source,
            "table_name": table_name,
            "load_success": load_results.get("success", False),
            "target_table": load_results.get("table_id"),
            "rows_loaded": load_results.get("rows_loaded", 0),
            "bytes_loaded": load_results.get("bytes_loaded", 0)
        }

        logger.info(
            f"Loaded {load_results.get('rows_loaded', 0)} rows to BigQuery table {load_results.get('table_id')}",
            extra={"extra_fields": extra_fields}
        )

    def log_ge_expectation_results(logger: logging.Logger, data_source: str, table_name: str,
                               environment: str, expectation_results: List[Dict[str, Any]]):
        """Log individual Great Expectations results."""

        for result in expectation_results:
            extra_fields = {
                "event_type": "ge_expectation",
                "data_source": data_source,
                "table_name": table_name,
                "environment": environment,
                "expectation_type": result.get("expectation_type"),
                "success": result.get("success"),
                "element_count": result.get("result", {}).get("element_count"),
                "unexpected_count": result.get("result", {}).get("unexpected_count"),
                "unexpected_percent": result.get("result", {}).get("unexpected_percent")
            }

            message = f"GE Check: {result.get('expectation_type')} - {'PASSED' if result.get('success') else 'FAILED'}"
            level = logging.INFO if result.get('success') else logging.WARNING

            logger.log(level, message, extra={"extra_fields": extra_fields})

    def log_ge_validation_summary(logger: logging.Logger, data_source: str, table_name: str,
                                environment: str, validation_result: Dict[str, Any]):
        """Log Great Expectations validation summary."""
        statistics = validation_result.get("statistics", {})

        extra_fields = {
            "event_type": "ge_validation_summary",
            "data_source": data_source,
            "table_name": table_name,
            "environment": environment,
            "successful_expectations": statistics.get("successful_expectations"),
            "failed_expectations": statistics.get("unsuccessful_expectations"),
            "success_percent": statistics.get("success_percent"),
            "evaluated_expectations": statistics.get("evaluated_expectations")
        }

        success_rate = statistics.get("success_percent", 0)
        message = f"GE Validation Complete: {success_rate:.1f}% passed ({statistics.get('successful_expectations', 0)} of {statistics.get('evaluated_expectations', 0)} expectations)"

        logger.info(message, extra={"extra_fields": extra_fields})

    def log_error(logger: logging.Logger, error: Exception, context: Dict[str, Any] = None):
        """Log errors with structured context."""

        extra_fields = {
            "event_type": "error",
            "error_type": type(error).__name__,
            "error_message": str(error)
        }

        if context:
            extra_fields["error_context"] = context

        logger.error(
            f"Error occurred: {str(error)}",
            extra={"extra_fields": extra_fields},
            exc_info=True
        )

    def create_operation_logger(operation_name: str) -> logging.Logger:
        """Create a logger for a specific operation."""
        logger_name = f"{os.getenv('DATA_SOURCE', 'unknown')}.{operation_name}"
        return get_logger(logger_name)