import requests
from huggingface_hub import hf_hub_download


def get_instruction_from_hub(repo_id: str) -> str:
    # Download file
    file_path = hf_hub_download(repo_id, filename="instruction.txt", repo_type="model")

    # Read file
    with open(file_path, "r", encoding="utf-8") as file:
        instruction = file.read()

    return instruction


instruction = get_instruction_from_hub("dataesr/openchat-3.6-8b-acknowledgments")  # TODO: model from env ?


def chatml_messages(texts: list[str]) -> list:
    # Format texts to chatml
    messages = [[{"role": "system", "content": instruction}, {"role": "user", "content": text}] for text in texts]
    return messages


def predict(url: str, texts: list[str]):
    """
    Request on inference app to generate prediction

    Args:
        url (str): url of the inference app
        messages (list): list of messages

    Returns:
        predictions (list[str]): list of predictions
    """
    # Format to chatml with instruction from model
    messages = chatml_messages(texts)

    # Request model api
    body = {"messages": messages, "use_chatml": True}
    response = requests.post(f"{url}/predict", data=body)
    response.raise_for_status()
    json = response.json()

    return json.get("prediction")
