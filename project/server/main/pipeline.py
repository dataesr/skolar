import pandas as pd
import os
from project.server.main.harvester.test import process_publication
from project.server.main.grobid import parse_grobid
from project.server.main.inference.acknowledgement import detect_acknowledgement
from project.server.main.utils import (
    inference_app_run,
    inference_app_stop,
    id_to_string,
    cp_folder_local_s3,
    gzip_all_files_in_dir,
    get_elt_id,
    get_filename,
    get_lang
)
from project.server.main.mongo import get_oa
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_elts_from_dois(dois):
    return get_oa(dois) # get info from unpaywall db

def run_from_bso(bso_file, worker_idx, download=False, analyze=False, chunksize=100, early_stop = True):
    df = pd.read_json(bso_file, lines=True, chunksize=chunksize)
    for c in df:
        elts = c.to_dict(orient='records')
        if download:
            download_and_grobid(elts, worker_idx)
        if analyze:
            parse_paragraphs(elts, worker_idx)
        if early_stop:
            break

def download_and_grobid(elts, worker_idx):
    xml_paths = []
    for elt in elts:
        xml_path = process_publication(elt, worker_idx) # download + run_grobid
        if xml_path:
            xml_paths.append(xml_path)
    gzip_all_files_in_dir(f'/data/pdf_{worker_idx}')
    cp_folder_local_s3(f'/data/pdf_{worker_idx}', 'pdf')
    os.system(f'rm -rf /data/pdf_{worker_idx}')
    logger.debug(f'{len(xml_paths)} xmls extracted')
    return 

def parse_paragraphs(elts, worker_idx):
    paragraphs = []
    xml_paths = []
    for elt in elts:
        elt_id = get_elt_id(elt)
        xml_path = get_filename(elt_id, 'grobid')
        if os.path.isfile(xml_path):
            xml_paths.append(xml_path)
    logger.debug(f'{len(xml_paths)} / {len(elts)} files have an XML')
    nb_already_analyzed = 0
    for xml_path in xml_paths:
        uid = xml_path.split('/')[-1].split('.')[0]
        elt_id = id_to_string(uid)
        PARAGRAPH_TYPE = 'ACKNOWLEDGEMENT'
        filename_detection = get_filename(elt_id, PARAGRAPH_TYPE)
        if os.path.isfile(filename_detection):
            nb_already_analyzed += 1
        else:
            paragraphs += parse_grobid(xml_path, elt_id)
    logger.debug(f'{len(paragraphs)} new paragraphs extracted')
    logger.debug(f'{nb_already_analyzed} xmls already analyzed for {PARAGRAPH_TYPE}')
    detections = detect_acknowledgement(paragraphs)
    logger.debug(f'{len(detections)} paragraphs analyzed by llm')
    return detections
