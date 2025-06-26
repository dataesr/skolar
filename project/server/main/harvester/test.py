import re
import os
import requests
from project.server.main.harvester.wiley_client import WileyClient
from project.server.main.harvester.elsevier_client import ElsevierClient
from project.server.main.harvester.config import config
from project.server.main.harvester.download_publication_utils import publisher_api_download, standard_download, SUCCESS_DOWNLOAD, safe_instanciation_client, FAIL_DOWNLOAD
from project.server.main.grobid import run_grobid
from project.server.main.utils import get_filename
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

def process_entry(elt):
    global wiley_client, elsevier_client
    result = FAIL_DOWNLOAD
    elt_id = elt.get('id')
    doi = elt.get('doi')
    if doi and isinstance(doi, str):
        assert(doi.startswith('10.'))
        doi = doi.lower()
    if doi and not isinstance(elt_id, str):
        elt_id = f'doi{doi}'
        elt['id'] = elt_id
    filename = get_filename(elt_id, 'pdf')
    if os.path.isfile(filename):
        logger.debug(f'already downloaded {filename} for {elt_id}')
        return
    publisher = None
    if isinstance(elt.get('publisher_normalized'), str):
        publisher = elt['publisher_normalized']
    elif isinstance(elt.get('publisher'), str):
        publisher = elt['publisher']
    urls_to_test = []
    if doi and isinstance(doi, str):
        if wiley_client and 'wiley' in publisher.lower():
            result, _ = publisher_api_download(doi, filename, wiley_client)
        if elsevier_client and 'elsevier' in publisher.lower():
            result, _ = publisher_api_download(doi, filename, elsevier_client)
    if result == SUCCESS_DOWNLOAD:
        return
    oa_locations = []
    if isinstance(elt.get('oa_locations'), list):
        oa_locations = elt['oa_locations']
    if 'oa_details' in elt and isinstance(elt['oa_details'], dict):
        oa_dates = list(elt['oa_details'].keys())
        oa_dates.sort()
        last_oa_date = oa_dates[-1]
        if isinstance(elt['oa_details'].get(last_oa_date, {}).get('oa_locations'), list):
            oa_locations = elt['oa_details'][last_oa_date]['oa_locations']
    if len(oa_locations) == 0:
        logger.debug(f'no URL for download for {elt_id}')
        return 
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

def download_publication(elt, do_grobid = True):
    #elt = requests.get(f'https://api.unpaywall.org/v2/{doi}?email=unpaywall_01@example.com').json()
    process_entry(elt)
    elt_id = elt['id']
    pdf_file = get_filename(elt_id, 'pdf')
    if do_grobid and os.path.isfile(pdf_file):
        return run_grobid(pdf_file, get_filename(elt_id, 'grobid'))
