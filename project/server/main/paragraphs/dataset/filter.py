import os
import pandas as pd
from project.server.main.paragraphs.dataset.predict import is_dataset
from project.server.main.utils import get_filename, write_jsonl, get_make_data_count_labels
from project.server.main.logger import get_logger

logger = get_logger(__name__)

PARAGRAPH_TYPE = "dataset"
LANGS = ["en", "fr", "es", "pt", "it", "de"]

datasets = {}


def load_datasets():
    global datasets
    if not datasets:
        mdc_path = get_make_data_count_labels()
        mdc = pd.read_json(mdc_path, lines=True)
        datasets = {"mdc": mdc}


def make_data_count_is_dataset(paragraph, publication_id):
    found_citations = []

    if not datasets or "mdc" not in datasets:
        load_datasets()
    mdc_df = datasets["mdc"]

    if "id" not in mdc_df.columns or "datasets" not in mdc_df.columns:
        return found_citations

    dataset_labels = mdc_df[mdc_df.id == publication_id].datasets.values[0]
    if not isinstance(dataset_labels, list):
        return found_citations

    for label in dataset_labels:
        if label in paragraph["text"]:
            found_citations.append(label)
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
            logger.debug(f"found citations {found_citations} in a paragraph from {publication_id}")
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
