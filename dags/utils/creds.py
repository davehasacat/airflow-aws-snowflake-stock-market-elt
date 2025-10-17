"""
📦 creds_polygon.py  (or similar name under dags/utils/)

Utility for securely retrieving Polygon.io API keys from Airflow Variables
that are backed by AWS Secrets Manager.

This allows the same code to work:
  - Locally (via `.env` environment variables)
  - In Airflow (via SecretsManagerBackend)
"""

import os
from airflow.models import Variable


def get_polygon_key(kind: str = "stocks") -> str:
    """
    Retrieve the Polygon.io API key for either stocks or options.

    Precedence order:
      1. Airflow Variable (AWS Secrets-backed) → via SecretsManagerBackend
      2. Environment variable fallback (for local testing)
      3. Raises RuntimeError if neither source provides a key

    Args:
        kind (str): Either "stocks" or "options".
                    Determines which secret/env var to look up.

    Returns:
        str: The resolved API key string.

    Raises:
        RuntimeError: If no valid key found in either Airflow or environment.

    Expected configuration:
      - In AWS Secrets Manager:
          airflow/variables/polygon_stocks_api_key
          airflow/variables/polygon_options_api_key
      - Locally (optional fallback):
          POLYGON_STOCKS_API_KEY
          POLYGON_OPTIONS_API_KEY
    """

    # Map key type to its Airflow Variable and fallback environment variable
    var_name = "polygon_stocks_api_key" if kind == "stocks" else "polygon_options_api_key"
    env_name = "POLYGON_STOCKS_API_KEY" if kind == "stocks" else "POLYGON_OPTIONS_API_KEY"

    # --- Primary: Airflow Variable (AWS Secrets-backed) ---
    try:
        val = Variable.get(var_name)  # Automatically resolved via SecretsManagerBackend
        if val:
            return val
    except Exception:
        # Intentionally silent fallback (Airflow not running or variable missing)
        pass

    # --- Secondary: Local .env fallback ---
    val = os.getenv(env_name, "")
    if val:
        return val

    # --- Failure: No key found anywhere ---
    raise RuntimeError(
        f"❌ Missing Polygon API key. "
        f"Set Secret 'airflow/variables/{var_name}' in AWS Secrets Manager "
        f"or define environment variable {env_name} for local use."
    )
