import re
import os
import requests
from project.server.main.harvester.wiley_client import WileyClient
from project.server.main.harvester.elsevier_client import ElsevierClient
from project.server.main.harvester.springer_client import SpringerClient
from project.server.main.harvester.config import config
from project.server.main.harvester.download_publication_utils import publisher_api_download, standard_download, SUCCESS_DOWNLOAD, safe_instanciation_client, FAIL_DOWNLOAD, proxy_download
from project.server.main.grobid import run_grobid
from project.server.main.utils import get_filename, get_elt_id, get_ip
from project.server.main.logger import get_logger
logger = get_logger(__name__)

current_ip = get_ip()
# Wiley
wiley_client = None
try:
    wiley_client = safe_instanciation_client(WileyClient, config['WILEY'])
except:
    logger.debug(f'instantiating WILEY client with ip {current_ip} FAILED')
# Elsevier
elsevier_client = None
try:
    elsevier_client = safe_instanciation_client(ElsevierClient, config['ELSEVIER'])
except:
    logger.debug(f'instantiating ELSEVIER client with ip {current_ip} FAILED')
# Springer
springer_client = None
try:
    springer_client = safe_instanciation_client(SpringerClient, config['SPRINGER'])
except:
    logger.debug(f'instantiating SPRINGER client with ip {current_ip} FAILED')

def process_entry(elt, worker_idx = 1, already_done = set()):
    global wiley_client, elsevier_client, springer_client
    result = FAIL_DOWNLOAD
    elt_id = get_elt_id(elt)
    elt['id'] = elt_id
    filename = get_filename(elt_id, f'pdf_{worker_idx}')
    filename_xml = get_filename(elt_id, 'grobid')
    filename_xml_publisher = get_filename(elt_id, 'publisher-xml')
    if os.path.isfile(filename_xml):
        logger.debug(f'already downloaded / grobidified {filename} for {elt_id}')
        return
    if os.path.isfile(filename_xml_publisher):
        logger.debug(f'already downloaded xml {filename_xml_publisher} for {elt_id}')
        return
    publisher = None
    publisher = None
    if isinstance(elt.get('publisher_normalized'), str):
        publisher = elt['publisher_normalized']
    elif isinstance(elt.get('publisher'), str):
        publisher = elt['publisher']
    urls_to_test = []
    logger.debug(f"try to download {elt['id']}")
    doi = elt.get('doi')
    if doi and isinstance(doi, str):
        if wiley_client and 'wiley' in publisher.lower():
            result, _ = publisher_api_download(doi, filename, wiley_client)
        if elsevier_client and 'elsevier' in publisher.lower():
            result, _ = publisher_api_download(doi, filename, elsevier_client)
        if springer_client and 'springer' in publisher.lower():
            result, _ = publisher_api_download(doi, filename_xml_publisher, springer_client)
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
        if not isinstance(url, str) and isinstance(oa_loc.get('url'), str) and '/document' in oa_loc.get('url'):
            url = oa_loc['url']
        logger.debug(url)
        if isinstance(url, str):
            for g in ['medihal-', 'media.hal']:
                if g in url:
                    logger.debug('medihal is skipped')
                    return
            result = FAIL_DOWNLOAD
            try:
                result, _ = standard_download(url, filename, elt_id)
                if result == SUCCESS_DOWNLOAD:
                    return
            except:
                pass
            #if elt_id.startswith('hal'):
            #    return
            #elif elt_id.startswith('nnt'):
            #    return
            #else: #TODO change to activate proxy!
            #    return
            #try:
            #    result, _ = proxy_download(url, filename, elt_id)
            #    if result == SUCCESS_DOWNLOAD:
            #        return
            #except:
            #    pass
        logger.debug('---')
    logger.debug(f'download failed for {elt_id}')

def process_publication(elt, worker_idx = 1, do_grobid = True):
    #elt = requests.get(f'https://api.unpaywall.org/v2/{doi}?email=unpaywall_01@example.com').json()
    process_entry(elt, worker_idx)
    elt_id = elt['id']
    pdf_file = get_filename(elt_id, f'pdf_{worker_idx}')
    if do_grobid and os.path.isfile(pdf_file):
        return run_grobid(pdf_file, get_filename(elt_id, 'grobid'))
