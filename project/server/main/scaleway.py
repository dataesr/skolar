import requests
import time
import json
from retry import retry
import os
from project.server.main.logger import get_logger

logger = get_logger(__name__)

SCW_URL = "https://api.scaleway.com/inference/v1/regions/fr-par"
SCW_SECRET_KEY = os.getenv("SCALEWAY_SECRET_KEY")

HEADERS = {
    "Authorization": f"Bearer {SCW_SECRET_KEY}",
    "Content-Type": "application/json",
}


def scaleway_is_deployed(deployment_url, model_name):
    try:
        res = requests.get(f"{SCW_URL}/deployments", headers=HEADERS)
        deployments = res.json()
        for deploy in deployments:
            endpoints = deploy.get("endpoints", [{}])
            for endpoint in endpoints:
                if endpoint.get("url", "") == deployment_url:
                    deploy_model_name = deploy.get("model_name", "")
                    if deploy_model_name != model_name:
                        logger.warning(f"Scaleway deployed with incorrect model ({deploy_model_name} != {model_name})")
                    return True
    except Exception as error:
        logger.error(f"Error while reaching Scaleway deployments: {str(error)}")
    return False


@retry(delay=30, tries=2, logger=logger)
def scaleway_agent_completion(text, deployment_url, model_name):
    # model_name = 'baguette-funders-600m-4k-with-template'
    URL = deployment_url + "/v1/chat/completions"
    t0 = time.time()

    messages = [{"content": text, "role": "user"}]

    # custom prompts #TODO: move this in paragraphs..
    if model_name in ["funding-extraction-llama-31-8b-instruct"]:
        prompt = f"Extract funding information from the following statement:\n  {text}"
        messages = [
            {
                "role": "system",
                "content": 'You are an expert at extracting structured funding metadata from academic papers. Given a funding statement, extract all funders and their associated awards. Return a JSON array of funder objects. Each funder has:\n- "funder_name": string or null\n- "awards": array of objects with "award_ids" (array of strings), "funding_scheme" (array of strings), and "award_title" (array of strings)\nReturn ONLY the JSON array, no other text.',
            },
            {"role": "user", "content": prompt},
        ]

    PAYLOAD = {
        "model": model_name,
        "messages": messages,
        "max_tokens": min(len(text.split(" ")) + 1500, 4000),
        "temperature": 0.0,
        "top_p": 0.95,
        "presence_penalty": 0,
        "stream": False,
        "reasoning_effort": "medium",
        "response_format": {"type": "text"},
    }

    response = requests.post(URL, headers=HEADERS, data=json.dumps(PAYLOAD), timeout=60)
    response.raise_for_status()
    payload = response.json()

    choices = payload.get("choices", [])
    if not choices:
        raise ValueError("Scaleway response had no choices")

    content = choices[0].get("message", {}).get("content", "")
    if not isinstance(content, str):
        raise ValueError("Scaleway response content is not a string")
    if not content.strip():
        raise ValueError("Scaleway response content is empty")

    t1 = time.time()
    logger.debug(f"This model call last {(t1 - t0)}")
    return content.strip()


def parse_llm_output(text: str) -> dict:
    raw_text = text.strip()

    # Find and parse json
    for start, ch in enumerate(raw_text):
        if ch not in "{[":
            continue
        try:
            parsed_cot = raw_text[:start]
            parsed_json = json.JSONDecoder().raw_decode(raw_text[start:])[0]
            if isinstance(parsed_json, list):
                return {"projects": parsed_json}  # specific to CDL model for now
            if isinstance(parsed_json, dict):
                output = parsed_json
                if parsed_cot:
                    output["CoT"] = parsed_cot
                return output
        except json.JSONDecodeError:
            continue

    # Raise error if no valid JSON is found
    raise Exception(f"Failed to parse JSON: \n{raw_text}")
