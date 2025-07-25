import os
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from project.server.main.pipeline import run_from_file
from project.server.main.logger import get_logger
from project.server.main.training.build_training import build_train_and_calibrate
from project.server.main.utils import get_bso_data, inference_app_run, inference_app_stop

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
    inference_app_stop('ACKNOWLEDGEMENT')
    return jsonify({'res': 'ok'}), 202

@main_blueprint.route("/train", methods=["POST"])
def run_train():
    args = request.get_json(force=True)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="skolar", default_timeout=default_timeout)
        task = q.enqueue(build_train_and_calibrate, args)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202

@main_blueprint.route("/process_bso", methods=["POST"])
def run_process_bso():
    args = request.get_json(force=True)
    # get_bso_data()
    logger.debug(f'splitting bso file in chunk of len 800 000 ; expect 5 files outputs')
    # split_bso_data()
    worker_idx = 1
    if args.get('analyze'):
        inference_app_run('ACKNOWLEDGEMENT')
    for f in os.listdir('/data/bso_chunks'):
        if f.startswith('chunk_bso'):
            assert(f in ['chunk_bso_aa', 'chunk_bso_ab', 'chunk_bso_ac', 'chunk_bso_ad', 'chunk_bso_ae'])
            with Connection(redis.from_url(current_app.config["REDIS_URL"])):
                q = Queue(name="skolar", default_timeout=default_timeout)
                task = q.enqueue(run_from_file, f'/data/bso_chunks/{f}', args, worker_idx)
                worker_idx += 1
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
