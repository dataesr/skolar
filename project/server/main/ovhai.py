import os
import json
import subprocess
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def ovhai_initialize():
    """
    Log in to ovhai cli
    """
    # ovhai login
    result = subprocess.run(
        ["ovhai", "login", "--username", os.getenv("OVHAI_USERNAME"), "--password-from-env", "OVHAI_PASSWORD"],
        shell=True,
        text=True,
    )
    logger.debug("result", result)
    result.check_returncode()
    logger.info("✅ OVHAI CLI initialized!")


def ovhai_app_get_data(app_id: str) -> object:
    """
    Get ovh ai app data as json

    Args:
    - app_id (str): app id

    Returns:
    - object: app data
    """
    # get app json data
    result = subprocess.run(["ovhai", "app", "get", app_id, "-o", "json"], shell=True, capture_output=True)
    logger.debug("result", result)
    result.check_returncode()

    # parse results
    data = json.loads(result.stdout)
    logger.debug("data", data)
    return data


def ovhai_app_start(app_id: str):
    """
    Start existing ovhai app

    Args:
    - app_id (str): app id
    """
    # start app
    result = subprocess.run(["ovhai", "app", "start", app_id], shell=True, text=True)
    result.check_returncode()


def ovhai_app_stop(app_id: str):
    """
    Stop existing ovhai app

    Args:
    - app_id (str): app id
    """
    # stop app
    result = subprocess.run(["ovhai", "app", "stop", app_id], shell=True, text=True)
    result.check_returncode()
