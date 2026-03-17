import os
from retry import retry
from project.server.main.utils import get_filename, write_jsonl
from project.server.main.logger import get_logger
from project.server.main.mistral import mistral_agent_completion

logger = get_logger(__name__)

PARAGRAPH_TYPE = "dataset"


@retry(delay=30, tries=2, logger=logger)
def dataset_llm_completions(publication_id, paragraphs) -> list:
    """
    Get LLM completions for dataset paragraphs.

    Args:
        publication_id (str): The ID of the publication.
        paragraphs (list): List of paragraphs to analyze.

    Returns:
        list: List of analyzed paragraphs with LLM completions.
    """
    analyzed_all = []
    filename_llm = get_filename(publication_id, PARAGRAPH_TYPE, "llm")

    for p in paragraphs:
        res = mistral_agent_completion(p["text"], os.getenv("MISTRAL_AGENT_DATASET_ID", ""))
        if res is None:
            continue
        try:
            analyzed = res
            analyzed["publication_id"] = p["publication_id"]
            analyzed["text"] = p["text"]
            analyzed_all.append(analyzed)
        except Exception as error:
            logger.debug(f"error parsing response from LLM : {res} ({error})")
            logger.debug(f"input was {p['text']}")
            continue

    write_jsonl(analyzed_all, filename_llm)
    return analyzed_all
