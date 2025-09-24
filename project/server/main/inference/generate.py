import requests
from retry import retry
import time
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def format_prompts(texts: list) -> list:
    """Format texts to prompts

    Args:
        texts (list): list of texts

    Returns:
        list: formatted prompts
    """
    # Format texts here if needed
    prompts = [text for text in texts]
    return prompts


def generate_pipeline(
    texts: list, inference_url: str, chat_template_params: dict = None, sampling_params: dict = None
) -> tuple[list, dict]:
    """Pipeline for generation of completions

    Args:
        texts (list): list of texts
        inference_url (str): inference app url
        chat_template_params (dict, optional): chat template additionnal params
        sampling_params (dict, optional): inference sampling params

    Returns:
        tuple[list, dict]: completions, task_data
    """
    # Format prompts
    prompts = format_prompts(texts)

    # Submit generation task
    task_id = generate_submit(prompts, inference_url, chat_template_params, sampling_params)
    logger.debug(f"for the {len(texts)} texts, task_id = {task_id}")

    # for tx, t in enumerate(texts):
    #    logger.debug(t)
    #    if tx > 5:
    #        break

    # Get generation task completions
    completions, task_data = generate_get_completions(task_id, inference_url)  # TODO: add timeout?
    logger.debug(f"got {len(completions)}")

    # for tx, t in enumerate(completions):
    #    logger.debug(t)
    #    if tx > 5:
    #        break

    return completions, task_data


def generate_submit(
    prompts: list, inference_url: str, chat_template_params: dict = None, sampling_params: dict = None
) -> str:
    """Submit a generation task

    Args:
        prompts (list): list of prompts
        inference_url (str): inference app url
        chat_template_params (dict, optional): chat template additionnal params
        sampling_params (dict, optional): inference sampling params

    Returns:
        str: submitted task id
    """
    submit_url = inference_url
    body = {"prompts": prompts}
    if chat_template_params:
        body["chat_template_params"] = chat_template_params
    if sampling_params:
        body["sampling_params"] = sampling_params

    response = requests.post(submit_url, json=body)
    response.raise_for_status()
    data = response.json()
    task_id = data.get("task_id")
    task_status = data.get("status")
    logger.debug(f"Generate task {task_id} created (state={task_status})")
    return task_id


@retry(delay=5, tries=3)
def get_safe(url):
    response = requests.get(url)
    response.raise_for_status()
    return response


def generate_get_completions(task_id: str, inference_url: str, timeout: int = None) -> tuple[list, dict]:
    """Get results of a generation task

    Args:
        task_id (str): task id
        inference_url (str): inference app url
        timeout (int, optional): timeout of catching task results

    Returns:
        tuple[list, dict]: completions, task_data
    """
    completions_url = f"{inference_url}/{task_id}"
    start_time = time.time()

    while True:
        response = get_safe(completions_url)
        data = response.json()
        task_time = int(time.time() - start_time)

        task_status = data.get("status")
        if task_status is None:
            logger.error(f"Generate task {task_id} not found!")
            raise KeyError(f"Generate task {task_id} not found")

        if task_status == "error":
            logger.error(f'Generate task {task_id} failed: {data.get("error")}')
            raise RuntimeError(f'Generate task {task_id} failed: {data.get("error")}')

        if task_status in ("queued", "running"):
            if timeout and (task_time > timeout):
                logger.warning(f"Generate task {task_id} took too long ({task_time}s), aborting...")
                raise RuntimeError(f"Generate task {task_id} took too long ({task_time}s)")
            logger.debug(f"Generate task {task_id} still {task_status}, retrying in 60s...")
            time.sleep(60)
            continue

        assert task_status == "done"
        completions = data.pop("completions")
        if not isinstance(completions, list):
            logger.error(f"Generate task {task_id} error: invalid completions format ({type(completions)})")
            raise ValueError(f"Generate task {task_id} error: invalid completions format ({type(completions)})")
        return completions, data
