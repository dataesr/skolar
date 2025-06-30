import pandas as pd
import os
from project.server.main.harvester.test import process_publication
from project.server.main.grobid import parse_grobid
from project.server.main.inference.acknowledgement import detect_acknowledgement
from project.server.main.utils import make_sure_model_stopped, make_sure_model_started, id_to_string, cp_folder_local_s3, gzip_all_files_in_dir
from project.server.main.mongo import get_oa
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_elts_from_dois(dois):
    return get_oa(dois) # get info from unpaywall db

def run_from_bso(bso_file, worker_idx):
    df = pd.read_json(bso_file, lines=True, chunksize=1000)
    for c in df:
        elts = c.to_dict(orient='records')
        run(elts, worker_idx)
        break

def run(elts, worker_idx):
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
    paragraphs = []
    for xml_path in xml_paths:
        uid = xml_path.split('/')[-1].split('.')[0]
        elt_id = id_to_string(uid)
        paragraphs += parse_grobid(xml_path, elt_id)
    logger.debug(f'{len(paragraphs)} paragraphs extracted')
    detections = detect_acknowledgement(paragraphs)
    logger.debug(f'{len(detections)} paragraphs detected')

    return detections


