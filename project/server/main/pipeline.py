from project.server.main.harvester.test import download_doi
from project.server.main.grobid import parse_grobid
from project.server.main.inference.acknowledgement import detect_acknowledgement
from project.server.main.utils import make_sure_model_stopped, id_to_string
from project.server.main.mongo import get_oa
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def run(dois):
    xml_paths = []
    elts = get_oa(dois) # get info from unpaywall db
    for elt in elts:
        xml_path = download_doi(elt) # download + run_grobid
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


