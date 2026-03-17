import os
import pandas as pd
from project.server.main.paragraphs.dataset.predict import is_dataset
from project.server.main.utils import get_filename, write_jsonl, download_file
from project.server.main.logger import get_logger

logger = get_logger(__name__)

PARAGRAPH_TYPE = "dataset"
LANGS = ["en", "fr", "es", "pt", "it", "de"]

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


def make_data_count_is_dataset(paragraph, publication_id):
    if datasets is None:
        load_datasets()
    mdc_df = datasets["mdc"]
    found_citations = []
    dataset_names = mdc_df[mdc_df.publication == publication_id].dataset.unique().tolist()
    for name in dataset_names:
        if name in paragraph["text"]:
            found_citations.append(name)

    return found_citations


def dataset_filter(publication_id, paragraphs) -> list:
    """
    Filter paragraphs to keep only the ones that are likely to be dataset.

    Args:
        publication_id (str): The ID of the publication.
        paragraphs (list): List of paragraphs to filter.

    Returns:
        filtered_paragraphs (list): List of filtered paragraphs.
    """
    filtered_paragraphs = []
    max_paragraph_len = 0

    filename_filtered = get_filename(publication_id, PARAGRAPH_TYPE, "filter")

    logger.debug(f"start filter {len(paragraphs)} paragraphs from {publication_id}")

    for paragraph in paragraphs:
        if paragraph["lang"] not in LANGS:
            # logger.debug(f'skip paragraph {paragraph} because of lang')
            continue
        found_citations = make_data_count_is_dataset(paragraph, publication_id)
        if found_citations:
            paragraph["make_data_count_citations"] = found_citations
            filtered_paragraphs.append(paragraph)
            continue
        if is_dataset(paragraph):
            if len(paragraph.get("text").split(" ")) < 10:
                continue
            filtered_paragraphs.append(paragraph)
            max_paragraph_len = max(max_paragraph_len, len(paragraph["text"]))
            if len(paragraph["text"]) > 2500:
                logger.debug("long paragraph:")
                logger.debug(paragraph)
                logger.debug("---")

    logger.debug(
        f"{len(filtered_paragraphs)} paragraphs kept after first {PARAGRAPH_TYPE} detection step - Max length = {max_paragraph_len}"
    )
    write_jsonl(filtered_paragraphs, filename_filtered)
    return filtered_paragraphs
