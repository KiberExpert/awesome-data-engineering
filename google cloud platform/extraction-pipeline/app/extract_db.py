# ==============================================================================
# # extract.py
# # SQL Server data extraction
# # ==============================================================================

import os
import logging
import pandas as pd
from typing import Dict, Any
from datetime import datetime
from logging_utils import get_logger

logger = get_logger(__name__)

def extract_data(data_source: str, table_name: str, environment: str, **kwargs) -> Dict[str, Any]:
    """
    Extract data from source SQL Server database.

    Args:
        data_source: Should be "source" for this container
        table_name: Table to extract from SQL Server
        environment: Environment (dev, staging, prod)
        **kwargs: Additional parameters from Pub/Sub message

    Returns:
        Dictionary with success status, data, and metadata
    """
    try:
        logger.info(f"Starting source SQL Server extraction for table: {table_name}")

        # This container only handles source data
        if data_source != "source":
            raise ValueError(f"This container only handles source data, received: {data_source}")

        return extract_source_data(table_name, environment, **kwargs)

    except Exception as e:
        logger.error(f"source extraction failed: {e}")
        return {"success": False, "error": str(e)}
    
def extract_source_data(table_name: str, environment: str, **kwargs) -> Dict[str, Any]:
    """Extract data from source SQL Server database."""
    try:
        import sqlalchemy
        import urllib.parse

        # Get SQL Server configuration from environment variables
        server = os.getenv("source_SERVER") # SQL Server hostname/IP
        port = os.getenv("source_PORT", "1433")
        database = os.getenv("source_DATABASE")
        username = os.getenv("source_USERNAME")
        password = os.getenv("source_PASSWORD")
        driver = os.getenv("source_DRIVER", "ODBC Driver 17 for SQL Server")

        # Validate required credentials
        if not all([server, database, username, password]):
            missing = [var for var, val in {
                "source_SERVER": server,
                "source_DATABASE": database,
                "source_USERNAME": username,
                "source_PASSWORD": password
            }.items() if not val]
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

        # URL encode password to handle special characters
        encoded_password = urllib.parse.quote_plus(password)
        encoded_username = urllib.parse.quote_plus(username)

        # Build SQL Server connection string
        connection_string = (
            f"mssql+pyodbc://{encoded_username}:{encoded_password}@"
            f"{server}:{port}/{database}?driver={urllib.parse.quote_plus(driver)}"
            f"&TrustServerCertificate=yes" # Handle SSL certificate issues
        )

        logger.info(f"Connecting to source SQL Server: {server}:{port}/{database}")
        
        # Create engine with connection pooling and timeout settings
        engine = sqlalchemy.create_engine(
            connection_string,
            pool_pre_ping=True, # Verify connections before use
            pool_recycle=3600, # Recycle connections every hour
            connect_args={
                "timeout": 30, # Connection timeout
                "autocommit": True
            }
        )

        # Build SQL query
        query = build_source_query(table_name, **kwargs)

        logger.info(f"Executing source query: {query}")

        # Execute query and load into DataFrame
        with engine.connect() as connection:
            df = pd.read_sql(query, connection)

        # Log success
        logger.info(f"Successfully extracted {len(df)} rows from source table: {table_name}")
        
        return {
            "success": True,
            "data": df,
            "rows_extracted": len(df),
            "extraction_timestamp": datetime.utcnow(),
            "source_metadata": {
                "server": server,
                "database": database,
                "table": table_name,
                "query": query,
                "connection_info": f"{server}:{port}/{database}"
            }
        }

    except sqlalchemy.exc.SQLAlchemyError as e:
        logger.error(f"SQL Server connection/query error: {e}")
        return {"success": False, "error": f"Database error: {str(e)}"}
    except Exception as e:
        logger.error(f"source extraction failed: {e}")
        return {"success": False, "error": str(e)}

def build_source_query(table_name: str, **kwargs) -> str:
    """
    Build SQL query for source table extraction.

    Args:
        table_name: Name of the source table
        **kwargs: Additional parameters for filtering

    Returns:
        SQL query string
    """
    # Base query
    query = f"SELECT * FROM {table_name}"

    # Add WHERE conditions based on parameters
    conditions = []

    # Date range filtering
    if "start_date" in kwargs:
        conditions.append(f"created_date >= '{kwargs['start_date']}'")

    if "end_date" in kwargs:
        conditions.append(f"created_date <= '{kwargs['end_date']}'")

    # Incremental loading based on last modified
    if "last_modified_after" in kwargs:
        conditions.append(f"last_modified > '{kwargs['last_modified_after']}'")

    # Filter by specific IDs if provided
    if "filter_ids" in kwargs and kwargs["filter_ids"]:
        id_list = ", ".join(str(id) for id in kwargs["filter_ids"])
        conditions.append(f"id IN ({id_list})")

    # Custom WHERE clause
    if "custom_where" in kwargs:
        conditions.append(kwargs["custom_where"])

    # Add WHERE clause if conditions exist
    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    # Add ORDER BY for consistent results
    if "order_by" in kwargs:
        query += f" ORDER BY {kwargs['order_by']}"
    else:
        # Default ordering by ID or created_date if available
        query += " ORDER BY CASE WHEN COLUMPROPERTY(OBJECT_ID(?), 'id', 'ColumnId') IS NOT NULL THEN id ELSE crea"  # TODO check the actual code

    # Add LIMIT if specified
    if "limit" in kwargs:
        query = f"SELECT TOP {kwargs['limit']} * FROM ({query}) AS limited_query"

    return query

def test_source_connection() -> Dict[str, Any]:
    """
    Test source database connection.
    
    Returns:
    Dictionary with connection test results
    """
    try:
        import sqlalchemy
        import urllib.parse

        # Get connection details
        server = os.getenv("source_SERVER")
        port = os.getenv("source_PORT", "1433")
        database = os.getenv("source_DATABASE")
        username = os.getenv("source_USERNAME")
        password = os.getenv("source_PASSWORD")

        if not all([server, database, username, password]):
            return {"success": False, "error": "Missing connection credentials"}

        # Build connection string
        encoded_password = urllib.parse.quote_plus(password)
        encoded_username = urllib.parse.quote_plus(username)

        connection_string = (
            f"mssql+pyodbc://{encoded_username}:{encoded_password}@"
            f"{server}:{port}/{database}?driver=ODBC+Driver17+for+SQL+Server"
            f"&TrustServerCertificate=yes"
        )

    # Test connection
    engine = sqlalchemy.create_engine(connection_string)

    with engine.connect() as connection:
        result = connection.execute(sqlalchemy.text("SELECT 1 AS test_connection"))
        test_value = result.fetchone()[0]

        if test_value == 1:
            logger.info(f"source connection test successful: {server}:{port}/{database}")
            return {
                "success": True,
                "message": "Connection successful",
                "server": f"{server}:{port}",
                "database": database
            }
        else:
            return {"success": False, "error": "Connection test failed"}

    except Exception as e:
        logger.error(f"source connection test failed: {e}")
        return {"success": False, "error": str(e)}