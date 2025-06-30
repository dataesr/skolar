import os
import requests
import redis

from flask import Blueprint, current_app, jsonify, render_template, request
from rq import Connection, Queue
from project.server.main.pipeline import run_from_bso
from project.server.main.logger import get_logger
from project.server.main.utils import get_bso_data
from project.server.main.utils import make_sure_model_stopped, make_sure_model_started

default_timeout = 4320000

logger = get_logger(__name__)

main_blueprint = Blueprint(
    "main",
    __name__,
)


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("home.html")

def split_bso_data():
    os.system('mkdir -p /data/bso_chunks && cd /data/bso_chunks && rm -rf chunk*')
    os.system(f'cd /data && zcat bso-publications-latest.jsonl.gz | split -l 800000 - chunk_bso_ && mv chunk_bso* bso_chunks/.')

@main_blueprint.route("/process_bso", methods=["POST"])
def run_process_bso():
    #make_sure_model_started('ACKNOWLEDGEMENT')
    #get_bso_data()
    logger.debug(f'splitting bso file in chunk of len 800 000 ; expect 5 files outputs')
    #split_bso_data()
    worker_idx = 1
    for f in os.listdir('/data/bso_chunks'):
        if f.startswith('chunk_bso'):
            assert(f in ['chunk_bso_aa', 'chunk_bso_ab', 'chunk_bso_ac', 'chunk_bso_ad', 'chunk_bso_ae'])
            with Connection(redis.from_url(current_app.config["REDIS_URL"])):
                q = Queue(name="skolar", default_timeout=default_timeout)
                task = q.enqueue(run_from_bso, f'/data/bso_chunks/{f}', worker_idx)
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
