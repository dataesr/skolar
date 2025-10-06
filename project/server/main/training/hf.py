import re
import os
import datetime
import requests
import numpy as np
import random
import pandas as pd
import time
from project.server.main.logger import get_logger
from project.server.main.utils import download_file, clean_dir
from project.server.main.s3 import client_s3, upload_s3
from project.server.main.paragraphs.dataset import is_dataset
from project.server.main.paragraphs.software import is_software
from project.server.main.paragraphs.acknowledgement import is_acknowledgement
from project.server.main.paragraphs.clinicaltrial import is_clinicaltrial
from project.server.main.training.build_training import tag_mentions, infer_type
logger = get_logger(__name__)

ALL_FIELDS = ['is_dataset', 'is_software', 'is_acknowledgement', 'is_clinicaltrial']
OPENALEX_API_KEY = os.getenv('OPENALEX_API_KEY')
OA_FIELDS = [
    "open_access",
    "locations",
    "publication_year",
    "type",
    "primary_topic",
    "is_retracted",
    "language"
]
OA_FIELDS_STR = ','.join(OA_FIELDS)
oa_cache = {}

def parse():
    parsed_data = []
    for input_filename in os.listdir('/data/training/hf/'):
        if 'enriched_sampled_parsed_' not in input_filename:
            continue
        logger.debug(f'reading {input_filename}')
        df = pd.read_json(f'/data/training/hf/{input_filename}', lines=True)
        data_tmp = df.to_dict(orient='records')
        parsed_data_tmp = []
        for e in data_tmp:
            new_elt = {}
            if not isinstance(e.get('locations'), list):
                continue
            for loc in e['locations']:
                if 'cc-by' in str(loc.get('license')):
                    new_elt['license'] = 'cc-by'
            if new_elt.get('license') is None:
                continue
            for f in ['text', 'doi', 'type', 'detected_lang', 'publication_year', 'is_dataset', 'is_software', 'is_acknowledgement', 'is_clinicaltrial']:
                new_elt[f] = e[f]
            if not isinstance(e['primary_topic'], dict):
                continue
            keep = False
            for f in ['is_dataset', 'is_software', 'is_acknowledgement', 'is_clinicaltrial']:
                if e[f]:
                    keep = True
            if keep is False:
                continue
            new_elt['field_name'] = e['primary_topic']['field']['display_name']
            new_elt['field_id'] = e['primary_topic']['field']['id']
            parsed_data_tmp.append(new_elt)
        logger.debug(f'{len(parsed_data_tmp)} elts kept over {len(data_tmp)}')
        parsed_data += parsed_data_tmp
    pd.DataFrame(parsed_data).to_csv('/data/training/hf/text_categorization.csv.gz', index=False)


def build_dataset(args):
    fields = ALL_FIELDS
    for input_filename in os.listdir('/data/training/raw_paragraphs_from_grobid/'):
        nb_idx = args.get('nb_idx')
        assert(isinstance(nb_idx, str))
        if f'sampled_parsed_{nb_idx}' not in input_filename:
            continue
        if os.path.isfile(f'/data/training/hf/enriched_{input_filename}'):
            continue
        local_filename = f'/data/training/raw_paragraphs_from_grobid/{input_filename}'
        data = pd.read_json(local_filename, lines=True).to_dict(orient='records')
        part_name = local_filename.split('/')[-1].split('.')[0]
        logger.debug(f'{len(data)} rows read for {part_name}')
        data = tag_mentions(data)
        for d in data:
            inferred_type = infer_type(d['text'])
            if inferred_type:
                d['type'] = inferred_type
        clean_data = [d for d in data if d.get('type') not in ['figure', 'too_long', 'table', 'annex', 'references', 'appendix']]
        for d in clean_data:
            doi = d['doi']
            openalex_infos = get_oa_simple(doi)
            d.update(openalex_infos)
            if 'is_dataset' in fields:
                d['is_dataset'] = is_dataset(d)
            if 'is_software' in fields:
                d['is_software'] = is_software(d)
            if 'is_acknowledgement' in fields:
                d['is_acknowledgement'] = is_acknowledgement(d)
            if 'is_clinicaltrial' in fields:
                d['is_clinicaltrial'] = is_clinicaltrial(d)
        df_training = pd.DataFrame(clean_data)
        df_training.to_json(f'/data/training/hf/enriched_{input_filename}', lines=True, orient='records')


def get_oa_simple(doi):
    global oa_cache
    if doi in oa_cache:
        return oa_cache[doi]
    url = f'https://api.openalex.org/works?filter=doi:{doi}&select={OA_FIELDS_STR}&api_key={OPENALEX_API_KEY}'
    time.sleep(0.2)
    try:
        res =  requests.get(url).json()['results']
        oa_cache[doi] = res[0]
        if len(oa_cache)%1000 == 0:
            logger.debug(f'oa cache has now {len(oa_cache)} elts')
        return res[0]
    except:
        pass
    return {}
