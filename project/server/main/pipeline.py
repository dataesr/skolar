import pandas as pd
import os
from project.server.main.harvester.test import process_publication
from project.server.main.grobid import parse_grobid
from project.server.main.inference.acknowledgement import detect_acknowledgement, analyze_acknowledgement, get_mistral_answer
from project.server.main.utils import (
    inference_app_run,
    inference_app_stop,
    id_to_string,
    cp_folder_local_s3,
    gzip_all_files_in_dir,
    get_elt_id,
    get_filename,
    get_lang,
    is_dowloaded,
    has_acknowledgement
)
from project.server.main.mongo import get_oa
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def enrich_with_metadata(df):
    df['doi'] = df['doi'].apply(lambda x:x.lower().strip())
    dois = [d for d in df.doi.unique().tolist() if isinstance(d, str)]
    extra_metadata = get_oa(dois) # get info from unpaywall db
    extra_dict = {}
    for e in extra_metadata:
        extra_dict[e['doi']] = e
    new_data = []
    for e in df.to_dict(orient='records'):
        if e['doi'] in extra_dict:
            e.update(extra_dict[e['doi']])
        new_data.append(e)
    return new_data

def validation():
    input_file = '/src/validation/validation_from_florian_naudet_constant_vinatier.csv'
    args = {'download': True, 'parse': True, 'detect': True}
    run_from_file(input_file = input_file, args = args, worker_idx = 1)
    df = pd.read_csv(input_file)
    data = []
    for e in df.to_dict(orient='records'):
        e['elt_id'] = 'doi' + e['doi'].lower().strip()
        e['is_downloaded'] = is_dowloaded(e['elt_id'])
        e['has_acknowledgement'] = has_acknowledgement(e['elt_id'])
        data.append(e)
    pd.DataFrame(data).to_csv('/data/validation.csv', index=False)

def run_from_file(input_file, args, worker_idx):
    download = args.get('download', False)
    parse = args.get('parse', False)
    detect = args.get('detect', False)
    analyze = args.get('analyze', False) # LLM
    chunksize = args.get('chunksize', 100)
    early_stop = args.get('early_stop', False)
    if ('jsonl' in input_file) or ('chunk_bso' in input_file):
        df = pd.read_json(input_file, lines=True, chunksize=chunksize)
    elif 'csv' in input_file:
        df = pd.read_csv(input_file, chunksize=chunksize)
    for c in df:
        cols = list(c.columns)
        elts, paragraphs, filtered_paragraphs = [], [], []
        if ('oa_details' not in cols) and ('oa_locations' not in cols):
            elts = enrich_with_metadata(c)
        else:
            elts = c.to_dict(orient='records')
        if download:
            download_and_grobid(elts, worker_idx)
        if parse:
            paragraphs = parse_paragraphs(elts)
        if paragraphs and detect:
            filtered_paragraphs = detect_acknowledgement(paragraphs)
        if filtered_paragraphs and analyze:
            for p in filtered_paragraphs:
                get_mistral_answer(p)
        #    detections = analyze_acknowledgement(filtered_paragraphs)
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

def parse_paragraphs(elts):
    paragraphs = []
    xml_paths = []
    for elt in elts:
        elt_id = get_elt_id(elt)
        xml_path = get_filename(elt_id, 'grobid')
        if os.path.isfile(xml_path):
            xml_paths.append(xml_path)
        else:
            xml_path = get_filename(elt_id, 'publisher-xml')
            if os.path.isfile(xml_path):
                xml_paths.append(xml_path)
    logger.debug(f'{len(xml_paths)} / {len(elts)} files have an XML')
    nb_already_analyzed = 0
    for xml_path in xml_paths:
        uid = xml_path.split('/')[-1].split('.')[0]
        elt_id = id_to_string(uid)
        PARAGRAPH_TYPE = 'ACKNOWLEDGEMENT'
        filename_detection = get_filename(elt_id, PARAGRAPH_TYPE, 'filter')
        if os.path.isfile(filename_detection):
            nb_already_analyzed += 1
        else:
            paragraphs += parse_grobid(xml_path, elt_id)
    logger.debug(f'{len(paragraphs)} new paragraphs extracted')
    logger.debug(f'{nb_already_analyzed} xmls already analyzed for {PARAGRAPH_TYPE}')
    return paragraphs
