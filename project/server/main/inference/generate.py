import requests
import time
import json
from retry import retry
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def format_prompts(texts: list, model_name: str = None, chat_template_params: dict = None) -> list:
    """Format texts to prompts

    Args:
        texts (list): list of texts
        model_name (str, optional): flag to format according to specific model
        chat_template_params (dict, optional): additional params

    Returns:
        list: formatted prompts
    """
    # Format texts here if needed
    if model_name in ["numind/NuExtract-1.5-tiny", "numind/NuExtract-1.5", "dataesr/NuExtract-2.0-2B-causalLM"]:
        if "nuextract_template" in chat_template_params:
            template = chat_template_params.pop("nuextract_template")
            template = json.dumps(json.loads(template), indent=4)
            prompts = [f"""### Template:\n{template}\n### Text:\n{text}\n""" for text in texts]
            logger.debug(f"Formatted prompts as NuExtract: {prompts[0]}")
            return prompts
        else:
            logger.error(f"Missing 'nuextract_template' to format prompts as NuExtract 1.5 models")
            raise KeyError(f"Missing 'nuextract_template' to format prompts as NuExtrac 1.5 models")

    if model_name in ["dataesr/NuExtract-2.0-2B-causalLM"]:
        if "nuextract_template" in chat_template_params:
            template = chat_template_params.pop("nuextract_template")
            template = json.dumps(json.loads(template), indent=4)
            prompts = [f"""# Template:\n{template}\n# Context:\n{text}\n""" for text in texts]
            logger.debug(f"Formatted prompts as NuExtract: {prompts[0]}")
            return prompts
        else:
            logger.error(f"Missing 'nuextract_template' to format prompts as NuExtract 2.0 models")
            raise KeyError(f"Missing 'nuextract_template' to format prompts as NuExtrac 2.0 models")

    prompts = [text for text in texts]
    return prompts


def generate_pipeline(
    texts: list, inference_url: str, chat_template_params: dict = None, sampling_params: dict = None, format_as: str = None
) -> tuple:
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
    prompts = format_prompts(texts, model_name=format_as, chat_template_params=chat_template_params)

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


def generate_get_completions(task_id: str, inference_url: str, timeout: int = None) -> tuple:
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
