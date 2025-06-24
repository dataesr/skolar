import pickle
import re
import os
import shutil
import datetime
import requests
from project.server.main.logger import get_logger
logger = get_logger(__name__)

import base64

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
    assert(file_type in ['pdf', 'grobid'])
    encoded_id = string_to_id(elt_id)
    path_prefix = f'/data/{file_type}/' + get_path_from_id(encoded_id) + '/'
    os.system(f'mkdir -p {path_prefix}')
    filename=None
    if file_type == 'pdf':
        filename = path_prefix + encoded_id + '.pdf'
    if file_type == 'grobid':
        filename = path_prefix + encoded_id + '.tei.xml'
    assert(isinstance(filename, str))
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
