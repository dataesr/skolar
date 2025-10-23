import pandas as pd
import json
import os
import pickle
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
    has_acknowledgement,
    read_jsonl
)
from project.server.main.mongo import get_oa
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_already_done(input_dir, reset=True):
    assert(input_dir in ['grobid', 'publisher-xml'])
    cache_filename = f'/data/already_done_{input_dir}.pkl'
    if reset is False:
        done = pickle.load(open(cache_filename, 'rb'))
        logger.debug(f'{len(done)} files already done for {input_dir}')
        return done
    done = []
    for e in os.walk(f'/data/{input_dir}'):
        if e[2]:
            done += e[2]
    done = set(done)
    logger.debug(f'recomputed: {len(done)} files already done for {input_dir}')
    pickle.dump(done, open(cache_filename, 'wb'))
    return done

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

def test():
    input_file = '/src/validation/validation_from_florian_naudet_constant_vinatier.csv'
    df = pd.read_csv(input_file)
    elts = df.to_dict(orient='records')
    paragraphs = parse_paragraphs(elts=elts, use_cache=True, use_llm=True)
    return paragraphs


def get_from_live_unpaywall(doi):
    url = f'https://api.unpaywall.org/v2/{doi}?email=unpaywall_01@example.com'
    return requests.get(url).json()

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
    os.system(f'mkdir -p /data/pdf_{worker_idx}')
    #done_grobid = get_already_done('grobid')
    download = args.get('download', False)
    parse = args.get('parse', False)
    use_cache = args.get('use_cache', True)
    use_llm = args.get('use_llm', False)
    chunksize = args.get('chunksize', 100)
    early_stop = args.get('early_stop', False)
    if ('jsonl' in input_file) or ('chunk_bso' in input_file):
        df = pd.read_json(input_file, lines=True, chunksize=chunksize)
    elif 'csv' in input_file:
        df = pd.read_csv(input_file, chunksize=chunksize)
    chunk_idx = 0
    for c in df:
        chunk_idx += 1
        logger.debug(f'NEW CHUNK {chunk_idx}')
        cols = list(c.columns)
        elts, paragraphs, filtered_paragraphs = [], [], []
        if ('oa_details' not in cols) and ('oa_locations' not in cols):
            elts = enrich_with_metadata(c)
        else:
            elts = c.to_dict(orient='records')
        if download:
            download_and_grobid(elts, worker_idx)
        if parse:
            paragraphs = parse_paragraphs(elts, use_cache, use_llm)
        if early_stop:
            break

def download_and_grobid(elts, worker_idx, already_done = set()):
    xml_paths = []
    for elt in elts:
        if elt.get('hal_docType') in ['VIDEO', 'video']:
            logger.debug(f"skip video {elt['id']}")
            continue
        xml_path = process_publication(elt, worker_idx, already_done) # download + run_grobid
        if xml_path:
            xml_paths.append(xml_path)
    gzip_all_files_in_dir(f'/data/pdf_{worker_idx}')
    cp_folder_local_s3(f'/data/pdf_{worker_idx}', 'pdf')
    os.system(f'rm -rf /data/pdf_{worker_idx}')
    logger.debug(f'{len(xml_paths)} xmls extracted')
    return 

def parse_paragraphs(elts, use_cache=True, use_llm = True):
    paragraphs = []
    xml_paths = []
    logger.debug(f'start going through paths for {len(elts)} elts')
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
    new_parsing, llm_call = 0, 0
    already_parsed, already_llm = 0, 0
    llm_res = []
    for xml_path in xml_paths:
        uid = xml_path.split('/')[-1].split('.')[0]
        elt_id = id_to_string(uid)
        PARAGRAPH_TYPE = 'ACKNOWLEDGEMENT'
        filename_paragraph = get_filename(elt_id, PARAGRAPH_TYPE, 'filter')
        filename_llm = get_filename(elt_id, PARAGRAPH_TYPE, 'llm')
        is_parsed, is_analyzed = False, False
        if use_cache and os.path.isfile(filename_paragraph):
            already_parsed += 1
            is_parsed = True
        if use_cache and os.path.isfile(filename_llm):
            already_llm += 1
            is_analyzed = True
        if (use_cache is False) or (is_parsed is False):
            new_parsing += 1
            paragraphs = parse_grobid(xml_path, elt_id)
            detect_acknowledgement(paragraphs)
        if use_llm:
            if (use_cache is False) or (is_analyzed is False):
                llm_call += 1
                llm_res += get_mistral_answer(elt_id)
            else:
                p_llm = read_jsonl(filename_llm)
                llm_res += p_llm
    logger.debug(f'already_parsed: {already_parsed}, already_llm: {already_llm}')
    logger.debug(f'new parsed: {new_parsing}, LLM calls: {llm_call}')
    return llm_res
