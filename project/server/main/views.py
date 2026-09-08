from numpy.ma import argsort
import os
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from project.server.main.inference.llm_compare import llm_compare
from project.server.main.pipeline import run_from_file
from project.server.main.logger import get_logger
from project.server.main.training.build_training import build_train_and_calibrate
from project.server.main.training.hf import build_dataset, parse
from project.server.main.utils import (
    get_bso_data,
    inference_app_run,
    inference_app_stop,
    sync_all,
    get_make_data_count_labels,
)

default_timeout = 4320000

logger = get_logger(__name__)

main_blueprint = Blueprint(
    "main",
    __name__,
)


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("home.html")


@main_blueprint.route("/stop", methods=["POST"])
def run_stop():
    inference_app_stop("acknowledgement")
    return jsonify({"res": "ok"}), 202


@main_blueprint.route("/hf", methods=["POST"])
def run_hf():
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="skolar", default_timeout=default_timeout)
        if args.get("build"):
            task = q.enqueue(build_dataset, args)
        if args.get("parse"):
            task = q.enqueue(parse, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/train", methods=["POST"])
def run_train():
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="skolar", default_timeout=default_timeout)
        task = q.enqueue(build_train_and_calibrate, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/sync", methods=["POST"])
def run_sync():
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="skolar", default_timeout=default_timeout)
        task = q.enqueue(sync_all, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/process_bso", methods=["POST"])
def run_process_bso():
    args = request.get_json(force=True)
    year = args.get("year")
    get_bso_data(year)
    worker_idx = 1
    for f in os.listdir("/data/bso_chunks"):
        if f.startswith(f"chunk_bso_{year}"):
            # assert(f in ['chunk_bso_aa', 'chunk_bso_ab', 'chunk_bso_ac', 'chunk_bso_ad', 'chunk_bso_ae', 'chunk_bso_af', 'chunk_bso_ag', 'chunk_bso_ah', 'chunk_bso_ai', 'chunk_bso_aj'])
            with Connection(redis.from_url(current_app.config["REDIS_URL"])):
                q = Queue(name="skolar", default_timeout=default_timeout)
                task = q.enqueue(run_from_file, f"/data/bso_chunks/{f}", args, f"{year}_{worker_idx}")
                worker_idx += 1
            response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/process_make_data_count", methods=["POST"])
def run_process_make_data_count():
    args = request.get_json(force=True)
    mdc_filename = get_make_data_count_labels()
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="skolar", default_timeout=default_timeout)
        task = q.enqueue(run_from_file, mdc_filename, args, 1)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("skolar")
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            "status": "success",
            "data": {
                "task_id": task.get_id(),
                "task_status": task.get_status(),
                "task_result": task.result,
            },
        }
    else:
        response_object = {"status": "error"}
    return jsonify(response_object)


@main_blueprint.route("/llm_compare", methods=["POST"])
def compare():
    args = request.get_json(force=True)
    logger.debug(f"llm_compare={args}")
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="skolar", default_timeout=default_timeout)
        task = q.enqueue(llm_compare, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202
