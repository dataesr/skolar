import os
import requests

def chatml_messages(texts: list, instruction) -> list:
    # Format texts to chatml
    messages = [[{"role": "system", "content": instruction}, {"role": "user", "content": text}] for text in texts]
    return messages


def predict(texts: list, inference_url, instruction):
    """
    Request on inference app to generate prediction

    Args:
        texts (list[str]): list of text inputs

    Returns:
        predictions (list[str]): list of predictions
    """
    # Format to chatml with instruction from model
    messages = chatml_messages(texts, instruction)

    # Request model api
    body = {"messages": messages, "use_chatml": True}
    response = requests.post(inference_url, data=body)
    if response.status_code != 200:
        logger.debug(inference_url)
        logger.debug(body)
        logger.debug(response.text)
    response.raise_for_status()
    json = response.json()

    return json.get("prediction")

