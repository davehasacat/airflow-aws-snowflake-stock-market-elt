import os
from airflow.models import Variable

def get_polygon_key(kind: str = "stocks") -> str:
    """
    Returns the Polygon API key from Airflow Variables (AWS Secrets-backed),
    with env-var fallback for local overrides.
    """
    var_name = "polygon_stocks_api_key" if kind == "stocks" else "polygon_options_api_key"
    env_name = "POLYGON_STOCKS_API_KEY" if kind == "stocks" else "POLYGON_OPTIONS_API_KEY"

    try:
        val = Variable.get(var_name)  # resolved via SecretsManagerBackend
        if val:
            return val
    except Exception:
        pass

    val = os.getenv(env_name, "")
    if val:
        return val

    raise RuntimeError(
        f"Missing Polygon API key. Set Secret 'airflow/variables/{var_name}' "
        f"or env {env_name} for fallback."
    )
