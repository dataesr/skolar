import requests
import time
import json
from retry import retry
import os
from project.server.main.logger import get_logger

logger = get_logger(__name__)

SCW_SECRET_KEY = os.getenv('SCALEWAY_SECRET_KEY')

HEADERS = {
    "Authorization": f"Bearer {SCW_SECRET_KEY}",
    "Content-Type": "application/json",
}

def scaleway_agent_completion(ack, deployment_url, model_name):
    #model_name = 'baguette-funders-600m-4k-with-template'
    URL = deployment_url + '/v1/chat/completions'
    t0 = time.time()

    messages = [{"content": ack, "role": "user"}]

    if model_name in ['funding-extraction-llama-31-8b-instruct']:
        prompt = f"""Extract funding information from the following statement:
    {ack}
    """
        messages = [
    {"role": "system", "content": "You are an expert at extracting structured funding metadata from academic papers. Given a funding statement, extract all funders and their associated awards. Return a JSON array of funder objects. Each funder has:\n- \"funder_name\": string or null\n- \"awards\": array of objects with \"award_ids\" (array of strings), \"funding_scheme\" (array of strings), and \"award_title\" (array of strings)\nReturn ONLY the JSON array, no other text."},
    {"role": "user", "content": prompt},
        ]

    PAYLOAD = {
        "model": model_name,
        "messages": messages,
        "max_tokens": min(len(ack.split(' '))+1500, 4000),
        "temperature": 0.0,
        "top_p": 0.95,
        "presence_penalty": 0,
        "stream": False,
        "reasoning_effort": "medium",
        "response_format": {"type": "text"},
    }

    response = requests.post(URL, headers=HEADERS, data=json.dumps(PAYLOAD))
    content = response.json().get("choices", [{}])[0].get("message", {}).get("content", "")
    t1 = time.time()
    logger.debug(f"This model call last {(t1-t0)}")
    return content

def parse_llm_output(text: str) -> dict:
    if text[0:1]=='[':
        try:
            # specific to CDL model for now
            return {'projects': json.loads(text)}
        except:
            return {}
    # Trouver le début du JSON (dernier '{' au niveau racine)
    json_start = text.rfind('\n{')
    if json_start == -1:
        json_start = text.rfind('{')

    cot = text[:json_start].strip()
    json_str = text[json_start:].strip()

    try:
        parsed_json = json.loads(json_str)
    except:
        parsed_json={}

    return {
        "CoT": cot,
        **parsed_json
    }
