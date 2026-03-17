import os
from retry import retry
from project.server.main.utils import get_filename, write_jsonl
from project.server.main.logger import get_logger
from project.server.main.mistral import mistral_agent_completion

logger = get_logger(__name__)

PARAGRAPH_TYPE = "acknowledgement"


def markdown_to_json(res_md):
    entities = []
    current_entity = {}
    for v in res_md.split("\n"):
        if v.startswith("**") and v.endswith("**"):
            if current_entity:
                entities.append(current_entity)
            current_entity = {"entity": v.replace("**", "").strip()}
        elif v.startswith("- Type:"):
            type_ = v.replace("- Type:", "").strip()
            current_entity["type"] = type_
            if type_ not in ["FUNDER", "INFRASTRUCTURE", "PRIVATE_COMPANY"]:
                logger.debug(type_)
        elif v.startswith("- Grant ID:"):
            grant_id = v.replace("- Grant ID:", "").strip()
            if len(grant_id) > 2:
                current_entity["grant_id"] = grant_id
        elif v.startswith("- Program:"):
            program = v.replace("- Program:", "").strip()
            if len(program) > 2:
                current_entity["program"] = program
    if current_entity:
        entities.append(current_entity)
    funders = [e for e in entities if e.get("type") == "FUNDER"]
    infrastructures = [e for e in entities if e.get("type") == "INFRASTRUCTURE"]
    private_companies = [e for e in entities if e.get("type") == "PRIVATE_COMPANY"]
    return {"raw_md": res_md, "funders": funders, "infrastructures": infrastructures, "private_companies": private_companies}


@retry(delay=30, tries=2, logger=logger)
def acknowledgement_llm_completions(publication_id, paragraphs) -> list:
    """
    Get LLM completions for paragraphs.

    Args:
        publication_id (str): The ID of the publication.
        paragraphs (list): List of paragraphs to analyze.

    Returns:
        list: List of analyzed paragraphs with LLM completions.
    """
    analyzed_all = []
    filename_llm = get_filename(publication_id, PARAGRAPH_TYPE, "llm")

    for p in paragraphs:
        res = mistral_agent_completion(p["text"], os.getenv("MISTRAL_AGENT_ACK_ID", ""))
        if res is None:
            continue
        try:
            analyzed = markdown_to_json(res)
            analyzed["publication_id"] = p["publication_id"]
            analyzed["text"] = p["text"]
            analyzed_all.append(analyzed)
        except Exception as error:
            logger.debug(f"error parsing response from LLM : {res} ({error})")
            logger.debug(f"input was {p['text']}")
            continue

    write_jsonl(analyzed_all, filename_llm)
    return analyzed_all
