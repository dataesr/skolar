import pandas as pd
from project.server.main.harvester.test import download_publication
from project.server.main.grobid import parse_grobid
from project.server.main.inference.acknowledgement import detect_acknowledgement
from project.server.main.utils import make_sure_model_stopped, make_sure_model_started, id_to_string
from project.server.main.mongo import get_oa
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_elts_from_dois(dois):
    return get_oa(dois) # get info from unpaywall db

def run_from_bso():
    make_sure_model_started('ACKNOWLEDGEMENT', wait=False)
    try:
        df = pd.read_json('/data/bso-publications-latest.jsonl.gz', lines=True, chunksize=100)
        for c in df:
            elts = c.to_dict(orient='records')
            run(elts)
            break
    except:
        pass
    make_sure_model_stopped('acknowledgement')

def run(elts):
    xml_paths = []
    for elt in elts:
        xml_path = download_publication(elt) # download + run_grobid
        if xml_path:
            xml_paths.append(xml_path)
    paragraphs = []
    logger.debug(f'{len(xml_paths)} xmls extracted')
    for xml_path in xml_paths:
        uid = xml_path.split('/')[-1].split('.')[0]
        elt_id = id_to_string(uid)
        paragraphs += parse_grobid(xml_path, elt_id)
    logger.debug(f'{len(paragraphs)} paragraphs extracted')
    detections = detect_acknowledgement(paragraphs)
    logger.debug(f'{len(detections)} paragraphs detected')
    make_sure_model_stopped('acknowledgement')

    return detections


