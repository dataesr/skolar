import pickle
import re
import os
import shutil
import datetime
import fasttext
import requests
import base64
import time
from huggingface_hub import hf_hub_download
from project.server.main.ovhai import ovhai_app_get_data, ovhai_app_start, ovhai_app_stop
from project.server.main.inference.predict import predict
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def gzip_all_files_in_dir(mydir):
    n = 0
    for root, dirs, files in os.walk(mydir):
        for f in files:
            os.system(f'cd {root} && gzip {f}')
            n += 1
    logger.debug(f'gzipped {n} files in {mydir}')

def cp_folder_local_s3(folder_local, folder_distant=None):
    if folder_distant is None:
        folder_distant = folder_local
    cmd = f'aws s3 cp {folder_local} s3://skolar/{folder_distant}  --recursive'
    logger.debug(f'cp_folder_local_s3 for {folder_local} to {folder_distant} cmd={cmd}')
    os.system(cmd)

def get_instruction_from_hub(repo_id: str) -> str:
    # Download file
    file_path = hf_hub_download(repo_id, filename="instruction.txt", repo_type="model")

    # Read file
    with open(file_path, "r", encoding="utf-8") as file:
        instruction = file.read()

    return instruction

def get_model_status(PARAGRAPH_TYPE):
    INFERENCE_APP_DATA = ovhai_app_get_data(os.getenv(f"{PARAGRAPH_TYPE.upper()}_INFERENCE_APP_ID"))
    return INFERENCE_APP_DATA['status']['state']

def make_sure_model_started(PARAGRAPH_TYPE, wait=True):
    logger.debug(f'make sure app {PARAGRAPH_TYPE} is running')
    INFERENCE_APP_DATA = ovhai_app_get_data(os.getenv(f"{PARAGRAPH_TYPE.upper()}_INFERENCE_APP_ID"))
    INFERENCE_APP_ID = f"{INFERENCE_APP_DATA.get('id')}"
    INFERENCE_APP_URL = f"{INFERENCE_APP_DATA.get('status', {}).get('url')}/predict"
    INFERENCE_APP_MODEL = next((env.get("value") for env in INFERENCE_APP_DATA.get("spec", {}).get("envVars", []) if env.get("name") == "MODEL_NAME"), None)
    instruction = get_instruction_from_hub(INFERENCE_APP_MODEL)
    logger.debug(f'current status = {get_model_status(PARAGRAPH_TYPE)}')
    if get_model_status(PARAGRAPH_TYPE) == 'RUNNING':
        try:
            predict(['This is a test'], INFERENCE_APP_URL, instruction)
        except:
            logger.debug(f'app {PARAGRAPH_TYPE} {INFERENCE_APP_ID} running but not yet workable, wait another 5 min...')
            time.sleep(60*5)
        return
    if get_model_status(PARAGRAPH_TYPE) == 'INITIALIZING':
        time.sleep(60*10)
    ovhai_app_start(INFERENCE_APP_ID)
    if wait:
        logger.debug(f'wait 10 min for the app {PARAGRAPH_TYPE} {INFERENCE_APP_ID} to start')
        time.sleep(60*10)

def make_sure_model_stopped(PARAGRAPH_TYPE):
    logger.debug(f'make sure app {PARAGRAPH_TYPE} is stopped')
    logger.debug(f'current status = {get_model_status(PARAGRAPH_TYPE)}')
    if get_model_status(PARAGRAPH_TYPE) == 'STOPPED':
        return
    INFERENCE_APP_DATA = ovhai_app_get_data(os.getenv(f"{PARAGRAPH_TYPE.upper()}_INFERENCE_APP_ID"))
    INFERENCE_APP_ID = f"{INFERENCE_APP_DATA.get('id')}"
    ovhai_app_stop(INFERENCE_APP_ID)
    time.sleep(10)

def get_bso_data():
    url = 'https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/bso_dump/bso-publications-latest.jsonl.gz'
    download_file(url, '/data/bso-publications-latest.jsonl.gz')

def get_models(PARAGRAPH_TYPE):
    model_path = f'/data/models/is_{PARAGRAPH_TYPE}/model_is_{PARAGRAPH_TYPE}_1M.ftz'
    if not os.path.isfile(model_path):
        download_file(f'https://skolar.s3.eu-west-par.io.cloud.ovh.net/models/is_{PARAGRAPH_TYPE}/model_is_{PARAGRAPH_TYPE}_1M.ftz', f'/data/models/is_{PARAGRAPH_TYPE}/model_is_{PARAGRAPH_TYPE}_1M.ftz')
        download_file(f'https://skolar.s3.eu-west-par.io.cloud.ovh.net/models/is_{PARAGRAPH_TYPE}/model_is_{PARAGRAPH_TYPE}_1M.vec', f'/data/models/is_{PARAGRAPH_TYPE}/model_is_{PARAGRAPH_TYPE}_1M.vec')
    fasttext_model = fasttext.load_model(model_path)
    INFERENCE_APP_DATA = ovhai_app_get_data(os.getenv(f"{PARAGRAPH_TYPE.upper()}_INFERENCE_APP_ID"))
    INFERENCE_APP_URL = f"{INFERENCE_APP_DATA.get('status', {}).get('url')}/predict"
    INFERENCE_APP_MODEL = next((env.get("value") for env in INFERENCE_APP_DATA.get("spec", {}).get("envVars", []) if env.get("name") == "MODEL_NAME"), None)
    instruction = get_instruction_from_hub(INFERENCE_APP_MODEL)
    return {'fasttext_model': fasttext_model, 'instruction': instruction, 'inference_url': INFERENCE_APP_URL}

def string_to_id(s):
    # Encoder la chaîne en bytes, puis en base64
    encoded_bytes = base64.b64encode(s.encode('utf-8'))
    encoded_str = encoded_bytes.decode('utf-8')
    # Remplacer les caractères non alphanumériques par une chaîne vide et convertir en minuscules
    id = re.sub(r'[^a-zA-Z0-9]', '', encoded_str)
    return id

def id_to_string(id):
    # Ajouter des caractères '=' pour que la longueur soit un multiple de 4
    padding = len(id) % 4
    if padding:
        id = id + '=' * (4 - padding)
    # Décoder l'id de base64 en bytes, puis en chaîne
    return base64.b64decode(id.encode('utf-8')).decode('utf-8')

def get_path_from_id(id):
    s1 = id[-2:].lower()
    s2 = id[-4:-2].lower()
    s3 = id[-6:-4].lower()
    s4 = id[-8:-6].lower()
    return f'{s1}/{s2}/{s3}/{s4}'

def get_filename(elt_id, file_type):
    #assert(file_type in ['pdf', 'grobid', 'acknowledgement'])
    encoded_id = string_to_id(elt_id)
    path_type = file_type
    if file_type in ['acknowledgement', 'software', 'dataset']:
        path_type = f'llm/{file_type}'
    path_prefix = f'/data/{path_type}/' + get_path_from_id(encoded_id) + '/'
    os.system(f'mkdir -p {path_prefix}')
    filename=None
    if file_type.startswith('pdf'):
        filename = path_prefix + encoded_id + '.pdf'
    if file_type == 'grobid':
        filename = path_prefix + encoded_id + '.tei.xml'
    if file_type in ['acknowledgement']:
        filename = path_prefix + encoded_id + '.acknowledgement.jsonl'
    assert(isinstance(filename, str))
    os.system(f'mkdir -p {path_prefix}')
    return filename


def get_filename_from_cd(cd: str):
    """ Get filename from content-disposition """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]

def download_file(url: str, destination: str = None) -> str:
    start = datetime.datetime.now()
    with requests.get(url, stream=True, verify=False) as r:
        r.raise_for_status()
        try:
            local_filename = get_filename_from_cd(r.headers.get('content-disposition')).replace('"', '')
        except:
            local_filename = url.split('/')[-1]
        logger.debug(f'Start downloading {local_filename} at {start}')
        local_filename = f'/data/{local_filename}'
        if destination:
            local_filename = destination
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f, length=16 * 1024 * 1024)
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'End download in {delta}')
    return local_filename

def clean_dir(directory):
    for k in [' ', '*']:
        assert(k not in directory)
    os.system(f'rm -rf {directory} && mkdir -p {directory}')
