import os
import requests
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def mistral_agent_completion(text: str, agent_id: str):
    messages = [{"role": "user", "content": text}]
    try:
        r = requests.post(
            os.getenv("MISTRAL_COMPLETION_URL", ""),
            json={"messages": messages, "agent_id": agent_id},
            headers={
                "Authorization": "Bearer " + os.getenv("MISTRAL_API_KEY", ""),
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
    except Exception as error:
        logger.error(f"Error in request to LLM : {error}")
        return None
    try:
        res = r.json()["choices"][0]["message"]["content"]
        return res
    except Exception as error:
        logger.error(f"Error in response from LLM : {r.text} ({error})")
        logger.error(f"Input was {text}")
        return None
