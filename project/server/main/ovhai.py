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
    cmd = f'ovhai login --username {os.getenv("OVHAI_USERNAME")} --password-from-env OVHAI_PASSWORD'
    result = subprocess.run(cmd, shell=True, text=True)
    result.check_returncode()
    logger.info("✅ OVHAI CLI initialized!")


def ovhai_app_get_data(app_id: str) -> dict:
    """
    Get ovh ai app data as json

    Args:
    - app_id (str): app id

    Returns:
    - object: app data
    """
    # get app json data
    cmd = f"ovhai app get {app_id} -o json"
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    result.check_returncode()
    data = json.loads(result.stdout)
    return data


def ovhai_app_update_env(app_id: str, env_name: str, env_value: str):
    """
    Update env variable of existing ovhai app

    Args:
    - app_id (str): app id
    """
    cmd = f"ovhai app update {app_id} --env {env_name}={env_value}"
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True)
    result.check_returncode()
    data = json.loads(result.stdout)

    # check modification is ok
    envs = data["spec"]["envVars"]
    for env in envs:
        if env["name"] == env_name and env["value"] == env_value:
            logger.debug(f"Successfully updated app env {env_name}")
            return data
    logger.error(f"Error while updating app environment {env_name}")

    return None


def ovhai_app_start(app_id: str):
    """
    Start existing ovhai app

    Args:
    - app_id (str): app id
    """
    # start app
    cmd = f"ovhai app start {app_id}"
    result = subprocess.run(cmd, shell=True, text=True)
    result.check_returncode()


def ovhai_app_stop(app_id: str):
    """
    Stop existing ovhai app

    Args:
    - app_id (str): app id
    """
    # stop app
    cmd = f"ovhai app stop {app_id}"
    result = subprocess.run(cmd, shell=True, text=True)
    result.check_returncode()
