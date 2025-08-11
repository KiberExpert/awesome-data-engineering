# main.py
# Entry point for the Cloud Run service; orchestrates the ingestion flow when triggered.
# ==============================================================================

import os
import json
import logging
from flask import Flask, request
from google.cloud import error_reporting
from pubsub_handler import handle_pubsub_message
from logging_utils import setup_logging

app = Flask(__name__)
setup_logging()
logger = logging.getLogger(__name__)

# Get data source name from environment
DATA_SOURCE = os.getenv("DATA_SOURCE")

@app.route("/", methods=["POST"])
def pubsub_endpoint():
    """Handle Pub/Sub messages for this specific data source."""
    try:
        pubsub_request = request.get_json()
        if not pubsub_request:
            logger.error("No Pub/Sub message received")
            return "Bad Request: no Pub/Sub message received", 400

        # Process the message for this data source
        result = handle_pubsub_message(pubsub_request, DATA_SOURCE)

        if result.get("success"):
            logger.info(f"Pipeline completed successfully: {result}")
            return f"Success: {result.get('message')}", 200
        else:
            logger.error(f"Pipeline failed: {result}")
            return f"Error: {result.get('error')}", 500

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        error_reporting.Client().report_exception()
        return f"Internal error: {str(e)}", 500

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "data_source": DATA_SOURCE,
        "service": "service_name"
    }, 200

    if __name__ == "__main__":
        port = int(os.environ.get("PORT", 8080))
        app.run(host="0.0.0.0", port=port)