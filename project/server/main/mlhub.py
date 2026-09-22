import os
import requests
from urllib.parse import quote_plus
from retry import retry
from project.server.main.logger import get_logger

logger = get_logger(__name__)

MLH_API_URL = os.getenv("ML_HUB_API_URL", "")

HEADERS = {
    "Authorization": os.getenv("ML_HUB_API_KEY", ""),
    "Content-Type": "application/json",
}


@retry(delay=30, tries=2, logger=logger)
def mlh_get_tool(tool: str, **kwargs):
    """Use ml-hub 'get' tool"""
    URL = MLH_API_URL + "/tools/" + tool + "?"
    for key, value in kwargs.items():
        URL += f"{key}={quote_plus(value)}&"

    logger.debug(f"Ml-hub URL = {URL}")

    response = requests.get(URL, headers=HEADERS, timeout=60)
    response.raise_for_status()

    data = response.json()
    return data
