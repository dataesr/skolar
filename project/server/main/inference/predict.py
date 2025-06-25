import os
import requests
from huggingface_hub import hf_hub_download
from project.server.main.ovhai import ovhai_app_get_data

def get_instruction_from_hub(repo_id: str) -> str:
    # Download file
    file_path = hf_hub_download(repo_id, filename="instruction.txt", repo_type="model")

    # Read file
    with open(file_path, "r", encoding="utf-8") as file:
        instruction = file.read()

    return instruction

INFERENCE_APP_DATA = ovhai_app_get_data(os.getenv("ACKNOWLEDGEMENT_INFERENCE_APP_ID"))
INFERENCE_APP_URL = f"{INFERENCE_APP_DATA.get("status", {}).get("url")}/predict"
INFERENCE_APP_MODEL = next((env.get("value") for env in INFERENCE_APP_DATA.get("spec", {}).get("envVars", []) if env.get("name") == "MODEL_NAME"), None)
instruction = get_instruction_from_hub(INFERENCE_APP_MODEL)

def chatml_messages(texts: list[str]) -> list:
    # Format texts to chatml
    messages = [[{"role": "system", "content": instruction}, {"role": "user", "content": text}] for text in texts]
    return messages


def predict(texts: list[str]):
    """
    Request on inference app to generate prediction

    Args:
        texts (list[str]): list of text inputs

    Returns:
        predictions (list[str]): list of predictions
    """
    # Format to chatml with instruction from model
    messages = chatml_messages(texts)

    # Request model api
    body = {"messages": messages, "use_chatml": True}
    response = requests.post(INFERENCE_APP_URL, data=body)
    response.raise_for_status()
    json = response.json()

    return json.get("prediction")
