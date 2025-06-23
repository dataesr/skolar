import requests


def predict(url: str, messages: list):
    """
    Request on inference app to generate prediction

    Args:
        url (str): url of the inference app
        messages (list): list of messages

    Returns:
        predictions (list[str]): list of predictions
    """
    body = {"messages": messages, "use_chatml": True}
    response = requests.post(f"{url}/predict", data=body)
    response.raise_for_status()
    json = response.json()
    return json.get("prediction")
