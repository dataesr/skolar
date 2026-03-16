import pickle
import re
import os
import fasttext
import requests
import json
import pandas as pd
from retry import retry
from project.server.main.paragraphs.dataset import is_dataset
from project.server.main.utils import get_models, get_filename, write_jsonl, read_jsonl, download_file
from project.server.main.logger import get_logger
logger = get_logger(__name__)

PARAGRAPH_TYPE = "dataset"

models = None
datasets = None


def load_datasets():
    global datasets
    if datasets is None:
        mdc_file = "make_data_count_citations_filtered.csv"
        mdc_path = f"/data/dataset/{mdc_file}"
        mdc_remote_path = f"https://skolar.s3.eu-west-par.io.cloud.ovh.net/datasets/{mdc_file}"
        if not os.path.isfile(mdc_path):
            download_file(mdc_remote_path, mdc_path)
        mdc = pd.read_csv(mdc_path, sep=";")
        datasets = {"mdc": mdc}


def make_data_count_is_dataset(p, publication_id: str):
    if datasets is None:
        load_datasets()
    mdc_df = datasets["mdc"]
    dataset_names = mdc_df[mdc_df.publication == publication_id].dataset.unique().tolist()
    for name in dataset_names:
        if name in p["text"]:
            return True

    return False


def infere_is_dataset(p, model):
    if is_dataset(p):
        return True
    prediction = model.predict(p['text'])
    proba = prediction[1][0]
    if prediction[0][0] == '__label__is_dataset' and proba > 0.5:
        return True

    return False


def filter_datasets(paragraphs):
    LANGS = ["en", "fr", "es", "pt", "it", "de"]
    global models
    if models is None:
        models = get_models(PARAGRAPH_TYPE)
    publi_id_map = {}
    logger.debug("start predictions")
    filtered_paragraphs = []
    max_paragraph_len = 0
    publi_id_map = {}
    for paragraph in paragraphs:
        publi_id = paragraph["publication_id"]
        if publi_id not in publi_id_map:
            publi_id_map[publi_id] = []
        if paragraph["lang"] not in LANGS:
            # logger.debug(f'skip paragraph {paragraph} because of lang')
            continue
        if make_data_count_is_dataset(paragraph, publi_id):
            filtered_paragraphs.append(paragraph)
            publi_id_map[publi_id].append(paragraph)
            continue
        if infere_is_dataset(paragraph, models["fasttext_model"]):
            if len(paragraph.get("text").split(" ")) < 10:
                continue
            filtered_paragraphs.append(paragraph)
            publi_id_map[publi_id].append(paragraph)
            max_paragraph_len = max(max_paragraph_len, len(paragraph["text"]))
            if len(paragraph["text"]) > 2500:
                logger.debug("long paragraph:")
                logger.debug(paragraph)
                logger.debug("---")
    logger.debug(
        f"{len(filtered_paragraphs)} paragraphs kept after first {PARAGRAPH_TYPE} detection step - Max length = {max_paragraph_len}"
    )
    for publi_id in publi_id_map:
        filename = get_filename(publi_id, PARAGRAPH_TYPE, "filter")
        write_jsonl(publi_id_map[publi_id], filename)
    return filtered_paragraphs


@retry(delay=30, tries=2, logger=logger)
def llm_answer_dataset(publication_id):
    filename_paragraph = get_filename(publication_id, PARAGRAPH_TYPE, "filter")
    paragraphs, analyzed_all = [], []
    try:
        paragraphs = read_jsonl(filename_paragraph)
    except:
        logger.debug(f"error loading filename_paragraph {filename_paragraph}")
        os.system(f"rm -rf {filename_paragraph}")
        return {}
    for p in paragraphs:
        messages = [{"role": "user", "content": p["text"]}]
        r = requests.post(
            os.getenv("MISTRAL_COMPLETION_URL"),
            json={"messages": messages, "agent_id": os.getenv("MISTRAL_AGENT_DATASET_ID")},
            headers={
                "Authorization": "Bearer " + os.getenv("MISTRAL_API_KEY"),
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )
        try:
            res_raw = r.json()["choices"][0]["message"]["content"]
            analyzed = json.loads(res_raw)
            analyzed["publication_id"] = p["publication_id"]
            analyzed["text"] = p["text"]
            analyzed_all.append(analyzed)
        except:
            logger.debug(f"error in response from LLM : {r.text}")
            logger.debug(f"input was {p['text']}")
            continue
    filename = get_filename(publication_id, PARAGRAPH_TYPE, "llm")
    write_jsonl(analyzed_all, filename)
    return analyzed_all
