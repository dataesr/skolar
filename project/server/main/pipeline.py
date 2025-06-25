from project.server.main.harvester.test import download_doi
from project.server.main.grobid import parse_grobid
from project.server.main.inference.acknowledgement import detect_acknowledgement
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def run(dois):
    xml_paths = []
    for doi in dois:
        xml_path = download_doi(doi) # download + run_grobid
        if xml_path:
            xml_paths.append(xml_path)
    paragraphs = []
    logger.debug(f'{len(xml_paths)} xmls extracted')
    for xml_path in xml_paths:
        paragraphs += parse_grobid(xml_path)
    logger.debug(f'{len(paragraphs)} paragraphs extracted')
    detections = detect_acknowledgement(paragraphs)
    logger.debug(f'{len(detections)} paragraphs detected')
    return detections


