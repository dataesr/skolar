import os
import pandas as pd
from project.server.main.paragraphs.software.predict import is_software
from project.server.main.utils import get_filename, write_jsonl
from project.server.main.logger import get_logger

logger = get_logger(__name__)

PARAGRAPH_TYPE = "software"
LANGS = ["en", "fr", "es", "pt", "it", "de"]


def software_filter(publication_id, paragraphs) -> list:
    """
    Filter paragraphs to keep only the ones that are likely to be software.

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
        if is_software(paragraph):
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
