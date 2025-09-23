import os
import json
import time
import pandas as pd
from datasets import load_dataset
from project.server.main.inference.generate import generate_pipeline
from project.server.main.logger import get_logger
from project.server.main.ovhai import ovhai_app_start, ovhai_app_update_env
from project.server.main.s3 import upload_s3
from project.server.main.utils import inference_app_get_id, inference_app_run

logger = get_logger(__name__)

BUCKET = "llm-completions"


def get_dataset_from_hf(dataset_name: str, dataset_split: str) -> pd.DataFrame:
    logger.info(f"Start loading huggingface dataset {dataset_name} (split={dataset_split})")

    dataset = load_dataset(dataset_name, split=dataset_split)

    if dataset:
        logger.debug(f"✅ Dataset {dataset_name} loaded!")
        logger.debug(f"Dataset schema: {dataset.features}")
        logger.debug(f"Dataset size: {len(dataset)}")
        logger.debug(f"Dataset sample: {dataset[0]}")
    else:
        logger.error(f"Error while loading {dataset_name}")
        raise Exception(f"Error while loading {dataset_name}")

    return dataset.to_pandas()


def model_start_app(model_name: str) -> str:
    # Get base inforence app id
    app_id = inference_app_get_id("BASE")

    # Update env of base inference app
    app_data = ovhai_app_update_env(app_id, env_name="MODEL_NAME", env_value=model_name)
    if not app_data:
        logger.error(f"Error while updating app environment")
        raise Exception(f"Error wile updating app environment")
    inference_url = f"{app_data.get('status', {}).get('url')}/generate"

    # Launch base app
    inference_app_run("BASE")
    logger.info(f"✅ Successfully launched inference app for {model_name =}")

    return inference_url


def write_results(df: pd.DataFrame, config: dict):
    current_time = time.strftime("%Y%m%d_%H%M")
    export_path = os.path.join(BUCKET, config["model_name"], current_time)
    config_path = os.path.join(export_path, "config.json")
    completions_path = os.path.join(export_path, "completions.json")

    # Write files to disk
    if df and config:
        df.to_json(completions_path, orient="records")
        with open(config_path, "w") as file:
            json.dump(config, file)

        # Export files to s3
        upload_s3("llm-finetuning", completions_path, completions_path)
        upload_s3("llm-finetuning", config_path, config_path)

        logger.debug("Successfully saved dataframe and config json file")


def model_inference(args: dict):
    model_name = args["model_name"]
    dataset_name = args["dataset_name"]
    dataset_split = args.get("dataset_split", "eval")
    instruction = args["instruction"]
    chat_template = args.get("chat_template")
    sampling_params = args.get("sampling_params")

    config = {
        "model_name": model_name,
        "dataset_name": dataset_name,
        "dataset_split": dataset_split,
        "instruction": instruction,
        "chat_template": chat_template,
        "sampling_params": sampling_params,
    }
    chat_template_params = {"instruction": instruction, "chat_template": chat_template}

    logger.info(f"▶️ Start inference of model {model_name}")
    logger.debug(f"{dataset_name =}, {dataset_split =}")

    # Get dataset_from_huggingface
    dataset = get_dataset_from_hf(dataset_name, dataset_split)
    texts = dataset["inputs"].to_list()
    config["dataset_len"] = len(dataset)

    # Load inference app
    inference_url = model_start_app(model_name)

    # Generate completions
    generation_start_t = time.time()
    completions = generate_pipeline(texts, inference_url, chat_template_params, sampling_params)
    config["duration"] = time.time() - generation_start_t

    # Check completions
    assert isinstance(completions, list)
    if len(completions) != len(texts):
        logger.error(f"Generated {len(completions)} completions from {len(texts)} texts")
        res = pd.DataFrame({"completions": completions})
        dataset = pd.concat([dataset, res])
    else:
        logger.info(f"✅ Generated {len(completions)}")
        dataset["completions"] = pd.Series(completions)

    # Write results on s3
    write_results(dataset, config)
