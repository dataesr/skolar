import os
from retry import retry
from project.server.main.utils import get_filename, write_jsonl
from project.server.main.logger import get_logger
from project.server.main.mistral import mistral_agent_completion
from project.server.main.scaleway import scaleway_agent_completion, parse_llm_output
from project.server.main.flair import flair_predict_entities

logger = get_logger(__name__)

PARAGRAPH_TYPE = "acknowledgement"
FLAIR_MODEL_NAME = "flair"

@retry(delay=30, tries=2, logger=logger)
def acknowledgement_llm_completions(publication_id, paragraphs, SCALEWAY_AGENT_ACK_ID, MODEL_NAME) -> list:
    """
    Get LLM completions for paragraphs.

    Args:
        publication_id (str): The ID of the publication.
        paragraphs (list): List of paragraphs to analyze.

    Returns:
        list: List of analyzed paragraphs with LLM completions.
    """ 
    analyzed_all = []
    failed_all = []
    filename_llm = get_filename(publication_id, PARAGRAPH_TYPE, f"llm_{MODEL_NAME}")

    for ixp, p in enumerate(paragraphs):

        logger.debug(f"Publication {publication_id} - paragraph {ixp+1}/{len(paragraphs)} --> llm call")
        analyzed = {"publication_id": publication_id, "text": p["text"]}

        try:
            # FLAIR NER
            if MODEL_NAME.lower() == FLAIR_MODEL_NAME:
                entities = flair_predict_entities(p["text"])
                analyzed["entities"] = entities
                analyzed_all.append(analyzed)

            # INFERENCE
            else:
                res = scaleway_agent_completion(p["text"], SCALEWAY_AGENT_ACK_ID, MODEL_NAME)
                output_dict = parse_llm_output(res)
                analyzed.update(output_dict)

            logger.debug(analyzed)
            analyzed_all.append(analyzed)

        except Exception as error:
            failed_all.append({**analyzed, "error": str(error)})
            logger.warning(f"Publication {publication_id} - paragraph {ixp+1} --> {error}")
            logger.debug(f" Paragraph: {p['text']}")
            continue

    write_jsonl(analyzed_all, filename_llm)
    write_jsonl(failed_all, filename_llm.replace(".jsonl", "_failed.jsonl"))
    logger.debug(f"Publication {publication_id}: \
        {len(analyzed_all)}/{len(paragraphs)} paragraphs succeeded (failed={len(failed_all)})")

    return analyzed_all
