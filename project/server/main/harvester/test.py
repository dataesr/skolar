import base64
import re
import os
import requests
from project.server.main.harvester.wiley_client import WileyClient
from project.server.main.harvester.elsevier_client import ElsevierClient
from project.server.main.harvester.config import config
from project.server.main.harvester.download_publication_utils import publisher_api_download, standard_download, SUCCESS_DOWNLOAD, safe_instanciation_client, FAIL_DOWNLOAD
from project.server.main.grobid import run_grobid
from project.server.main.logger import get_logger
logger = get_logger(__name__)

wiley_client, elsevier_client = None, None
try:
    wiley_client = safe_instanciation_client(WileyClient, config['WILEY'])
except:
    pass
try:
    elsevier_client = safe_instanciation_client(ElsevierClient, config['ELSEVIER'])
except:
    pass


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
    s1 = id[0:2].lower()
    s2 = id[2:4].lower()
    s3 = id[4:6].lower()
    s4 = id[6:8].lower()
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

def process_entry(elt):
    global wiley_client, elsevier_client
    result = FAIL_DOWNLOAD
    elt_id = elt.get('id')
    doi = elt.get('doi')
    if doi:
        assert(doi.startswith('10.'))
        doi = doi.lower()
    if doi and elt_id is None:
        elt_id = f'doi{doi}'
        elt['id'] = elt_id
    filename = get_filename(elt_id, 'pdf')
    if os.path.isfile(filename):
        logger.debug(f'already downloaded {filename} for {elt_id}')
        return
    publisher = None
    if elt.get('publisher_normalized'):
        publisher = elt['publisher_normalized']
    elif elt.get('publisher'):
        publisher = elt['publisher']
    urls_to_test = []
    if doi:
        if wiley_client and publisher == 'Wiley':
            result, _ = publisher_api_download(doi, filename, wiley_client)
        if elsevier_client and publisher == 'Elsevier':
            result, _ = publisher_api_download(doi, filename, elsevier_client)
    if result == SUCCESS_DOWNLOAD:
        return
    oa_locations = []
    if isinstance(elt.get('oa_locations'), list):
        oa_locations = elt['oa_locations']
    if 'oa_details' in elt:
        oa_dates = list(elt['oa_details'].keys())
        oa_dates.sort()
        last_oa_date = oa_dates[-1]
        if isinstance(elt['oa_details'].get(last_oa_date, {}).get('oa_locations'), list):
            oa_locations = elt['oa_details'][last_oa_date]['oa_locations']
    for oa_loc in oa_locations:
        url = oa_loc.get('url_for_pdf')
        logger.debug(url)
        if isinstance(url, str):
            result = FAIL_DOWNLOAD
            try:
                result, _ = standard_download(url, filename, elt_id)
            except:
                continue
            logger.debug(result)
            if result == SUCCESS_DOWNLOAD:
                return
        logger.debug('---')
    logger.debug(f'download failed for {elt_id}')

def download_doi(doi, do_grobid = True):
    elt = requests.get(f'https://api.unpaywall.org/v2/{doi}?email=unpaywall_01@example.com').json()
    process_entry(elt)
    elt_id = elt['id']
    pdf_file = get_filename(elt_id, 'pdf')
    if do_grobid and os.path.isfile(pdf_file):
        run_grobid(pdf_file, get_filename(elt_id, 'grobid'))
