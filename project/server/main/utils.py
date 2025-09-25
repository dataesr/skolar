import pickle
import re
import os
import shutil
import datetime
import fasttext
import requests
import base64
import time
import fasttext
from huggingface_hub import hf_hub_download
from project.server.main.ovhai import ovhai_app_get_data, ovhai_app_start, ovhai_app_stop
from project.server.main.logger import get_logger

lid_model = fasttext.load_model('/src/project/server/main/lid.176.ftz')

logger = get_logger(__name__)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def get_lang(text):
    pred = lid_model.predict(text, 1)
    lang = pred[0][0].replace('__label__', '')
    proba = pred[1][0]
    return {'lang': lang, 'proba': proba}

def get_ip():
    ip = requests.get('https://api.ipify.org').text
    return ip

def get_elt_id(elt):
    elt_id = elt.get('id')
    doi = elt.get('doi')
    if doi and isinstance(doi, str):
        assert(doi.startswith('10.'))
        doi = doi.lower()
    if doi and not isinstance(elt_id, str):
        elt_id = f'doi{doi}'
    return elt_id

def gzip_all_files_in_dir(mydir):
    n = 0
    for root, dirs, files in os.walk(mydir):
        for f in files:
            os.system(f'cd {root} && gzip {f}')
            n += 1
    logger.debug(f'gzipped {n} files in {mydir}')

def download_from_s3(distant_file, local_path):
    cmd = f'aws s3 cp s3://skolar/{distant_file} {local_path}'
    logger.debug(f'download_from_s3 {cmd}')
    os.system(cmd)

def cp_folder_local_s3(folder_local, folder_distant=None):
    if folder_distant is None:
        folder_distant = folder_local
    cmd = f'aws s3 cp {folder_local} s3://skolar/{folder_distant}  --recursive'
    logger.debug(f'cp_folder_local_s3 for {folder_local} to {folder_distant} cmd={cmd}')
    os.system(cmd)


def inference_app_get_id(PARAGRAPH_TYPE: str) -> str:
    app_id = os.getenv(f"{PARAGRAPH_TYPE.upper()}_INFERENCE_APP_ID")
    assert isinstance(app_id, str)
    return app_id


def inference_app_get_state(PARAGRAPH_TYPE: str) -> str:
    INFERENCE_APP_DATA = ovhai_app_get_data(inference_app_get_id(PARAGRAPH_TYPE))
    return INFERENCE_APP_DATA['status']['state']


def inference_app_run(PARAGRAPH_TYPE: str, timeout: int = 60 * 15):
    """make sure inference app is running"""
    logger.debug(f'make sure app {PARAGRAPH_TYPE} is running')
    start_time = time.time()

    while True:
        app_state = inference_app_get_state(PARAGRAPH_TYPE)
        logger.debug(f"current status = {app_state}")
        if app_state == "RUNNING":
            return

        duration = int(time.time() - start_time)
        if duration > timeout:
            logger.error(f"app { PARAGRAPH_TYPE} took too long to start, aborting...")
            raise RuntimeError(f"app { PARAGRAPH_TYPE} took too long to start, aborting...")

        if app_state in ("QUEUED", "INITIALIZING", "SCALING"):
            logger.debug(f"app {PARAGRAPH_TYPE} waiting to start...")
            time.sleep(60)
        if app_state in ("STOPPING", "STOPPED"):
            logger.debug(f"app {PARAGRAPH_TYPE} not started, restarting...")
            ovhai_app_start(inference_app_get_id(PARAGRAPH_TYPE))
            time.sleep(10)


def inference_app_stop(PARAGRAPH_TYPE: str):
    """make sure inference app is stopped"""
    logger.debug(f'make sure app {PARAGRAPH_TYPE} is stopped')
    logger.debug(f"current status = {inference_app_get_state(PARAGRAPH_TYPE)}")
    if inference_app_get_state(PARAGRAPH_TYPE) in ["FAILED", "STOPPING", "STOPPED"]:
        return
    ovhai_app_stop(inference_app_get_id(PARAGRAPH_TYPE))
    time.sleep(10)


def get_bso_data():
    url = 'https://storage.gra.cloud.ovh.net/v1/AUTH_32c5d10cb0fe4519b957064a111717e3/bso_dump/bso-publications-latest.jsonl.gz'
    download_file(url, '/data/bso-publications-latest.jsonl.gz')
    split_bso_data()

def split_bso_data():
    logger.debug(f'splitting bso file in chunk of len 800 000 ; expect 5 files outputs')
    os.system('mkdir -p /data/bso_chunks && cd /data/bso_chunks && rm -rf chunk*')
    os.system(f'cd /data && zcat bso-publications-latest.jsonl.gz | split -l 800000 - chunk_bso_ && mv chunk_bso* bso_chunks/.')

def get_models(PARAGRAPH_TYPE):
    model_path = f'/data/models/is_{PARAGRAPH_TYPE}/model_is_{PARAGRAPH_TYPE}_1M.ftz'
    if not os.path.isfile(model_path):
        download_file(f'https://skolar.s3.eu-west-par.io.cloud.ovh.net/models/is_{PARAGRAPH_TYPE}/model_is_{PARAGRAPH_TYPE}_1M.ftz', f'/data/models/is_{PARAGRAPH_TYPE}/model_is_{PARAGRAPH_TYPE}_1M.ftz')
        download_file(f'https://skolar.s3.eu-west-par.io.cloud.ovh.net/models/is_{PARAGRAPH_TYPE}/model_is_{PARAGRAPH_TYPE}_1M.vec', f'/data/models/is_{PARAGRAPH_TYPE}/model_is_{PARAGRAPH_TYPE}_1M.vec')
    fasttext_model = fasttext.load_model(model_path)
    INFERENCE_APP_DATA = ovhai_app_get_data(inference_app_get_id(PARAGRAPH_TYPE))
    INFERENCE_APP_URL = f"{INFERENCE_APP_DATA.get('status', {}).get('url')}/generate"
    return {"fasttext_model": fasttext_model, "inference_url": INFERENCE_APP_URL}

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

def is_dowloaded(elt_id):
    filename = get_filename(elt_id, 'grobid')
    return os.path.isfile(filename)

def has_acknowledgement(elt_id):
    filename = get_filename(elt_id, 'acknowledgement', 'filter')
    return os.path.isfile(filename)

def get_filename(elt_id, file_type_input, step=''):
    file_type = file_type_input.lower()
    #assert(file_type in ['pdf', 'grobid', 'acknowledgement'])
    encoded_id = string_to_id(elt_id)
    path_type = file_type
    if file_type in ['acknowledgement', 'software', 'dataset']:
        assert(step in ['llm', 'filter'])
        path_type = f'{step}/{file_type}'
    path_prefix = f'/data/{path_type}/' + get_path_from_id(encoded_id) + '/'
    os.system(f'mkdir -p {path_prefix}')
    filename=None
    if file_type.startswith('pdf'):
        filename = path_prefix + encoded_id + '.pdf'
    if file_type == 'grobid':
        filename = path_prefix + encoded_id + '.tei.xml'
    if file_type == 'publisher-xml':
        filename = path_prefix + encoded_id + '.publisher.xml'
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
