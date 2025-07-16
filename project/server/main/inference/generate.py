import requests
from retry import retry
import time
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def chatml_prompts(texts: list, instruction) -> list:
    # Format texts to chatml
    prompts = [[{"role": "system", "content": instruction}, {"role": "user", "content": text}] for text in texts]
    return prompts


def generate_pipeline(texts: list, inference_url: str, instruction: str):
    # Format to chatml with instruction from model
    prompts = chatml_prompts(texts, instruction)

    # Submit generation task
    task_id = generate_submit(prompts, inference_url)
    logger.debug(f'for the {len(texts)} texts, task_id = {task_id}')
    
    #for tx, t in enumerate(texts):
    #    logger.debug(t)
    #    if tx > 5:
    #        break

    # Get generation task completions
    completions = generate_get_completions(task_id, inference_url)  # TODO: add timeout?
    logger.debug(f'got {len(completions)}')
    
    #for tx, t in enumerate(completions):
    #    logger.debug(t)
    #    if tx > 5:
    #        break
    
    return completions


def generate_submit(prompts: list, inference_url: str) -> str:
    submit_url = inference_url
    body = {
        "prompts": prompts,
        "use_chatml": True,
        "sampling_params": {
            "repetition_penalty": 1.1,
            "frequency_penalty": 0.5,
            "presence_penalty": 0.1
        },
    }
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

def generate_get_completions(task_id: str, inference_url: str, timeout: int = None) -> list:
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
        completions = data.get("completions")
        if not isinstance(completions, list):
            logger.error(f"Generate task {task_id} error: invalid completions format ({type(completions)})")
            raise ValueError(f"Generate task {task_id} error: invalid completions format ({type(completions)})")
        return completions
