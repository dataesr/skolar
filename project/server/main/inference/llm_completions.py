import os
from retry import retry
from project.server.main.utils import get_filename, write_jsonl
from project.server.main.logger import get_logger
from project.server.main.mistral import mistral_agent_completion
from project.server.main.scaleway import scaleway_agent_completion, parse_llm_output

logger = get_logger(__name__)


@retry(delay=30, tries=2, logger=logger)
def llm_completions(
    publication_id: str,
    paragraphs: list,
    PARAGRAPH_TYPE: str,
    SCW_ENDPOINT: str,
    SCW_MODEL_NAME: str,
) -> list:
    """
    Get LLM completions for paragraphs.

    Args:
        publication_id (str): The ID of the publication.
        paragraphs (list): List of paragraphs to analyze.
        PARAGRAPH_TYPE (str): Type of paragraphs to analyze.
        SCW_ENDPOINT (str): Scaleway endpoint for inference.
        SCW_MODEL_NAME (str): Scaleway model name for inference.

    Returns:
        list: List of analyzed paragraphs with LLM completions.
    """
    analyzed_all = []
    failed_all = []
    filename_llm = get_filename(publication_id, PARAGRAPH_TYPE, f"llm_{SCW_MODEL_NAME}")

    for ixp, p in enumerate(paragraphs):
        logger.debug(f"Publication {publication_id} - paragraph {ixp+1}/{len(paragraphs)} --> llm call")
        analyzed = {"publication_id": publication_id, "text": p["text"]}
        raw_output = None
        try:
            raw_output = scaleway_agent_completion(p["text"], SCW_ENDPOINT, SCW_MODEL_NAME)
            output = parse_llm_output(raw_output)
            analyzed.update(output)
            analyzed_all.append(analyzed)
            # logger.debug(analyzed)
        except Exception as error:
            failed = {**analyzed, "error": str(error)}
            if raw_output is not None:
                failed["raw_output"] = raw_output
            failed_all.append(failed)
            logger.warning(f"Publication {publication_id} - paragraph {ixp+1} --> {error}")
            # logger.debug(f" LLM output: {raw_output}")
            continue

    write_jsonl(analyzed_all, filename_llm)
    write_jsonl(failed_all, filename_llm.replace(".jsonl", "_failed.jsonl"))
    logger.debug(f"Publication {publication_id}: \
        {len(analyzed_all)}/{len(paragraphs)} paragraphs succeeded (failed={len(failed_all)})")
    return analyzed_all
