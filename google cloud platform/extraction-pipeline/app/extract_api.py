# =============================================================================
# extract.py
# API data extraction
# =============================================================================

import os
import logging
import pandas as pd
import requests
from typing import Dict, Any
from datetime import datetime
from logging_utils import get_logger

logger = get_logger(__name__)

def extract_data(data_source: str, table_name: str, environment: str, **kwargs) -> Dict[str, Any]:
    """
    Extract data from source.

    Args:
        data_source: Should be "source" for this container
        table_name: Table/endpoint to extract
        environment: Environment (dev, staging, prod)
        **kwargs: Additional parameters from Pub/Sub message
    
    Returns:
        Dictionary with success status, data, and metadata
    """
    try:
        logger.info(f"Starting source extraction for table: {table_name}")

        # This container only handles source data
        if data_source != "source":
            raise ValueError(f"This container only handles source data, received: {data_source}")

        return extract_source_data(table_name, environment, **kwargs)

    except Exception as e:
        logger.error(f"source extraction failed: {e}")
        return {"success": False, "error": str(e)}

def extract_source_data(table_name: str, environment: str, **kwargs) -> Dict[str, Any]:
    """Extract data from source API."""
    try:
        # Get source configuration
        api_base_url = os.getenv("source_API_URL")
        api_key = os.getenv("source_API_KEY")
        username = os.getenv("source_USERNAME")
        password = os.getenv("source_PASSWORD")
        
    if not all([api_base_url, api_key]):
        raise ValueError("source API credentials not configured")

    # Build API endpoint
    endpoint = f"{api_base_url}/{table_name}"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    # Add any query parameters from kwargs
    params = {k: v for k, v in kwargs.items() if k.startswith("param_")}

    logger.info(f"Calling source API: {endpoint}")
    response = requests.get(
        endpoint,
        headers=headers,
        params=params,
        auth=(username, password) if username else None,
        timeout=300
    )
    response.raise_for_status()

    # Parse response
    data = response.json()
    
    # Convert to DataFrame
    if isinstance(data, dict) and "data" in data:
        df = pd.DataFrame(data["data"])
    elif isinstance(data, list):
        df = pd.DataFrame(data)
    else:
        df = pd.DataFrame([data])

    logger.info(f"Successfully extracted {len(df)} rows from source")

    return {
        "success": True,
        "data": df,
        "rows_extracted": len(df),
        "extraction_timestamp": datetime.utcnow(),
        "source_metadata": {
            "endpoint": endpoint,
            "response_size_bytes": len(response.content)
        }
    }

    except requests.exceptions.RequestException as e:
        logger.error(f"source API request failed: {e}")
        return {"success": False, "error": f"API request failed: {str(e)}"}
    except Exception as e:
        logger.error(f"source extraction failed: {e}")
        return {"success": False, "error":str(e)}