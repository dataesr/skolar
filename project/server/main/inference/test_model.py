import os
import json
import time
import pandas as pd
from datasets import load_dataset
from project.server.main.inference.generate import generate_pipeline
from project.server.main.logger import get_logger
from project.server.main.ovhai import ovhai_app_update_env
from project.server.main.s3 import upload_s3
from project.server.main.utils import inference_app_get_id, inference_app_run, inference_app_stop

logger = get_logger(__name__)

BUCKET = "skolar-completions"

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
    # Get base inference app id
    app_id = inference_app_get_id("BASE")

    # Make sure app is stopped
    inference_app_stop("BASE")

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
    export_path = os.path.join(f"./{BUCKET}", config["model_name"], current_time)
    if not os.path.exists(export_path):
        os.makedirs(export_path)

    config_path = os.path.join(export_path, "config.json")
    completions_path = os.path.join(export_path, "completions.json")

    # Write files to disk
    if len(df) and config:
        df.to_json(completions_path, orient="records")
        with open(config_path, "w") as file:
            json.dump(config, file)

        # Export files to s3
        upload_s3(BUCKET, completions_path, os.path.relpath(completions_path, f"./{BUCKET}"), is_public=None)
        upload_s3(BUCKET, config_path, os.path.relpath(config_path, f"./{BUCKET}"), is_public=None)

        logger.debug("Successfully saved dataframe and config json file")
        return

    logger.warning(f"Results not saved because df length={len(df)} or {config=}")


def model_inference(args: dict):
    model_name = args["model_name"]
    dataset_name = args["dataset_name"]
    dataset_split = args.get("dataset_split", "eval")
    prompts_params = args.get("prompts_params", {})
    sampling_params = args.get("sampling_params", {})

    config = {
        "model_name": model_name,
        "dataset_name": dataset_name,
        "dataset_split": dataset_split,
        "prompts_params": prompts_params or None,
        "sampling_params": sampling_params or None,
    }

    logger.info(f"▶️ Start inference of model {model_name}")
    logger.debug(f"{dataset_name =}, {dataset_split =}")

    # Get dataset_from_huggingface
    dataset = get_dataset_from_hf(dataset_name, dataset_split)
    texts = dataset["input"].to_list()
    config["dataset_len"] = len(dataset)

    # Load inference app
    inference_url = model_start_app(model_name)

    # Generate completions
    completions, task_data = generate_pipeline(texts, inference_url, prompts_params, sampling_params, model_name)
    config["duration"] = task_data.get("done_at") - task_data.get("running_at")

    # Check completions
    assert isinstance(completions, list)
    if len(completions) != len(texts):
        logger.error(f"Generated {len(completions)} completions from {len(texts)} texts")
        res = pd.DataFrame({"completions": completions})
        dataset = pd.concat([dataset, res])
    else:
        logger.info(f"✅ Generated {len(completions)}")
        dataset["completions"] = pd.Series(completions)

    # Stop inference app
    inference_app_stop("BASE")

    # Write results on s3
    write_results(dataset, config)
