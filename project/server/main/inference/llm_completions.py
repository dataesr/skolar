import os
from retry import retry
from project.server.main.utils import get_filename, write_jsonl
from project.server.main.logger import get_logger
from project.server.main.mistral import mistral_agent_completion
from project.server.main.scaleway import scaleway_get_completion, scaleway_get_data, scaleway_get_chat_completion
from project.server.main.mlhub import mlh_get_tool

logger = get_logger(__name__)


def get_completions(text: str, SCW_ENDPOINT: str, SCW_MODEL_NAME: str):
    """Build prompt and call correct completion fonction"""

    if SCW_MODEL_NAME in ["funding-extraction-llama-31-8b-instruct"]:
        prompt = f"Extract funding information from the following statement:\n  {text}"
        messages = [
            {
                "role": "system",
                "content": 'You are an expert at extracting structured funding metadata from academic papers. Given a funding statement, extract all funders and their associated awards. Return a JSON array of funder objects. Each funder has:\n- "funder_name": string or null\n- "awards": array of objects with "award_ids" (array of strings), "funding_scheme" (array of strings), and "award_title" (array of strings)\nReturn ONLY the JSON array, no other text.',
            },
            {"role": "user", "content": prompt},
        ]
        return scaleway_get_chat_completion(messages, SCW_ENDPOINT, SCW_MODEL_NAME)

    if SCW_MODEL_NAME in ["baguette-software-dataset"]:
        prompt = f"<|im_start|>user\n<text>{text}</text><|im_end|>\n<|im_start|>assistant\n"  # only extract
        return scaleway_get_completion(prompt, SCW_ENDPOINT, SCW_MODEL_NAME)

    if SCW_MODEL_NAME in ["baguette-funders-600m-4k"]:
        prompt = f"<|im_start|>user\n<text>{text}</text><|im_end|>\n<|im_start|>assistant\n<think>"  # only extract
        return scaleway_get_completion(prompt, SCW_ENDPOINT, SCW_MODEL_NAME)

    messages = [{"content": text, "role": "user"}]
    return scaleway_get_chat_completion(messages, SCW_ENDPOINT, SCW_MODEL_NAME)


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
            if SCW_MODEL_NAME.lower() == "flair":
                # Special case for flair (hosted by ml-hub)
                data = mlh_get_tool(tool="flair", text=p["text"])
            else:
                # Scaleway hosted models
                raw_output = get_completions(p["text"], SCW_ENDPOINT, SCW_MODEL_NAME)
                data = scaleway_get_data(raw_output)
            analyzed.update(data)
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
