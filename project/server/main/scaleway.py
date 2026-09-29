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
    "X-Auth-Token": SCW_SECRET_KEY,
    "Content-Type": "application/json",
}


def scaleway_is_deployed(deployment_url, model_name):
    try:
        res = requests.get(f"{SCW_URL}/deployments", headers=HEADERS)
        deployments = res.json()['deployments']
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
def scaleway_get_completion(prompt: str, deployment_url: str, model_name: str, **kwargs) -> str:
    """Get text completion from deployed model"""
    URL = deployment_url + "/v1/completions"
    t0 = time.time()

    PAYLOAD = {
        "model": model_name,
        "prompt": prompt,
        "max_tokens": kwargs.get("max_tokens", 2048),
        "temperature": kwargs.get("temperature", 0.0),
        "top_p": kwargs.get("top_p", 0.95),
        "presence_penalty": kwargs.get("presence_penalty", 0),
        "stream": False,
        "response_format": {"type": "text"},
    }

    if kwargs.get('stop'):
        PAYLOAD['stop'] = kwargs.get('stop')

    response = requests.post(URL, headers=HEADERS, json=PAYLOAD, timeout=kwargs.get('timeout', 120))
    response.raise_for_status()
    data = response.json()
    choices = data.get("choices") or [{}]
    content = choices[0].get("text")
    finish_reason = choices[0].get("finish_reason")

    if finish_reason == 'length':
        prompt_tokens = data.get("usage", {}).get("prompt_tokens", 1000)
        MAX_TOKEN = 4096 - prompt_tokens - 16
        PAYLOAD["max_tokens"] = MAX_TOKEN
        logger.debug(f"first attempt was too short on max_tokens {kwargs.get('max_tokens', 2048)}; retrying with some more")
        response = requests.post(URL, headers=HEADERS, json=PAYLOAD, timeout=kwargs.get('timeout', 120))
        response.raise_for_status()
        data = response.json()
        choices = data.get("choices") or [{}]
        content = choices[0].get("text")
        finish_reason = choices[0].get("finish_reason")

    if finish_reason == 'length':
        logger.debug("Scaleway response content truncated")
    if not isinstance(content, str):
        raise ValueError("Scaleway response content is not a string")
    if not content.strip():
        raise ValueError("Scaleway response content is empty")

    t1 = time.time()
    logger.debug(f"This model call last {(t1 - t0)}")
    return content.strip()


@retry(delay=30, tries=2, logger=logger)
def scaleway_get_chat_completion(messages: list, deployment_url: str, model_name: str, **kwargs) -> str:
    """Get text completion from deployed model with chat template"""
    URL = deployment_url + "/v1/chat/completions"
    t0 = time.time()

    # custom prompts #TODO: move this in paragraphs..
    # if model_name in ["funding-extraction-llama-31-8b-instruct"]:
    #     prompt = f"Extract funding information from the following statement:\n  {text}"
    #     messages = [
    #         {
    #             "role": "system",
    #             "content": 'You are an expert at extracting structured funding metadata from academic papers. Given a funding statement, extract all funders and their associated awards. Return a JSON array of funder objects. Each funder has:\n- "funder_name": string or null\n- "awards": array of objects with "award_ids" (array of strings), "funding_scheme" (array of strings), and "award_title" (array of strings)\nReturn ONLY the JSON array, no other text.',
    #         },
    #         {"role": "user", "content": prompt},
    #     ]
    # min(len(text.split(" ")) + 1500, 4000)

    PAYLOAD = {
        "model": model_name,
        "messages": messages,
        "max_tokens": kwargs.get("max_tokens", 2048),
        "temperature": kwargs.get("temperature", 0.0),
        "top_p": kwargs.get("top_p", 0.95),
        "presence_penalty": kwargs.get("presence_penalty", 0),
        "stream": False,
        "response_format": {"type": "text"},
    }

    response = requests.post(URL, headers=HEADERS, json=PAYLOAD, timeout=60)
    response.raise_for_status()

    data = response.json()
    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")

    if not isinstance(content, str):
        raise ValueError("Scaleway response content is not a string")
    if not content.strip():
        raise ValueError("Scaleway response content is empty")

    t1 = time.time()
    logger.debug(f"This model call last {(t1 - t0)}")
    return content.strip()

import json
import re

_DECODER = json.JSONDecoder()
_WS = " \n\r\t"


def _skip(s: str, pos: int, chars: str) -> int:
    while pos < len(s) and s[pos] in chars:
        pos += 1
    return pos


def _salvage_array(s: str, pos: int) -> list:
    """Éléments complets d'un tableau JSON tronqué commençant à s[pos] == '['."""
    items = []
    pos += 1
    while True:
        pos = _skip(s, pos, _WS + ",")
        if pos >= len(s) or s[pos] == "]":
            break
        try:
            item, pos = _DECODER.raw_decode(s, pos)
        except json.JSONDecodeError:
            break
        items.append(item)
    return items


def _salvage_object(s: str, pos: int) -> dict:
    """Paires clé/valeur complètes d'un objet JSON tronqué commençant à s[pos] == '{'.
    Si la valeur coupée est une liste, on garde ses éléments complets."""
    out = {}
    pos += 1
    while True:
        pos = _skip(s, pos, _WS + ",")
        if pos >= len(s) or s[pos] != '"':
            break
        try:
            key, pos = _DECODER.raw_decode(s, pos)
        except json.JSONDecodeError:
            break
        pos = _skip(s, pos, _WS + ":")
        if pos >= len(s):
            break
        try:
            value, pos = _DECODER.raw_decode(s, pos)
        except json.JSONDecodeError:
            if s[pos] == "[":
                items = _salvage_array(s, pos)
                if items:
                    out[key] = items
            break
        out[key] = value
    return out


def _is_plausible(parsed) -> bool:
    if isinstance(parsed, dict):
        return bool(parsed)
    if isinstance(parsed, list):
        return all(isinstance(x, dict) for x in parsed)
    return False


def _wrap(parsed, cot: str, truncated: bool = False) -> dict:
    output = {"projects": parsed} if isinstance(parsed, list) else parsed  # list: spécifique au modèle CDL
    if cot:
        output["CoT"] = cot
    if truncated:
        output["truncated"] = True
    return output


def scaleway_get_data(text: str) -> dict:
    raw_text = text.strip()

    for match in re.finditer(r"[{\[]", raw_text):
        start = match.start()
        cot = raw_text[:start].strip()

        # 1. JSON complet
        try:
            parsed, _ = _DECODER.raw_decode(raw_text, start)
            if _is_plausible(parsed):
                return _wrap(parsed, cot)
            continue
        except json.JSONDecodeError:
            pass

        # 2. Ça ressemble au début du vrai JSON ({" ou [{) mais c'est tronqué :
        #    on récupère ce qui est complet, sans jamais descendre dans les objets internes
        nxt = _skip(raw_text, start + 1, _WS)
        if raw_text[start] == "{" and raw_text[nxt:nxt + 1] == '"':
            parsed = _salvage_object(raw_text, start)
        elif raw_text[start] == "[" and raw_text[nxt:nxt + 1] == "{":
            parsed = [x for x in _salvage_array(raw_text, start) if isinstance(x, dict)]
        else:
            continue  # accolade ou crochet dans le raisonnement : on passe

        if parsed:
            return _wrap(parsed, cot, truncated=True)
        raise ValueError(f"JSON truncated before any complete element:\n{raw_text}")

    raise ValueError(f"Failed to parse JSON:\n{raw_text}")
