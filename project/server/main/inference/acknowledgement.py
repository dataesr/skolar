import pickle
import re
import os
import fasttext
import pandas as pd
from project.server.main.paragraphs.acknowledgement import is_acknowledgement
from project.server.main.inference.generate import generate_pipeline
from project.server.main.utils import download_file, clean_dir, get_models, inference_app_run, get_filename
from project.server.main.logger import get_logger
logger = get_logger(__name__)

PARAGRAPH_TYPE = 'acknowledgement'

models = None

def infere_is_acknowledgement(paragraph, fasttext_model):
    if is_acknowledgement(paragraph):
        return True
    txt = paragraph['text']
    prediction = fasttext_model.predict(txt)
    proba = prediction[1][0]
    if prediction[0][0] == f'label__is_{PARAGRAPH_TYPE}' and proba > 0.5:
        return True
    return False

def detect_acknowledgement(paragraphs):
    LANGS = ['en', 'fr', 'es', 'pt', 'it', 'de']
    global models
    if models is None:
        models = get_models(PARAGRAPH_TYPE)
    publi_id_map = {}
    logger.debug('start predictions')
    filtered_paragraphs = []
    max_paragraph_len = 0
    publi_id_map = {}
    for paragraph in paragraphs:
        if paragraph['lang'] not in LANGS:
            logger.debug(f'skip paragraph {paragraph} because of lang')
            continue
        publi_id = paragraph['publication_id']
        if publi_id not in publi_id_map:
            publi_id_map[publi_id] = []
        if infere_is_acknowledgement(paragraph, models['fasttext_model']):
            if len(paragraph.get('text').split(' ')) < 10:
                continue
            filtered_paragraphs.append(paragraph)
            publi_id_map[publi_id].append(paragraph)
            max_paragraph_len = max(max_paragraph_len, len(paragraph['text']))
            if len(paragraph['text']) > 2500:
                logger.debug('long paragraph:')
                logger.debug(paragraph)
                logger.debug('---')
    logger.debug(f'{len(filtered_paragraphs)} paragraphs kept after first {PARAGRAPH_TYPE} detection step - Max length = {max_paragraph_len}')
    for publi_id in publi_id_map:
        filename = get_filename(publi_id, PARAGRAPH_TYPE, 'filter')
        df_tmp = pd.DataFrame(publi_id_map[publi_id])
        if len(df_tmp):
            df_tmp.to_json(filename, orient='records', lines=True)
    return filtered_paragraphs

def analyze_acknowledgement(filtered_paragraphs):
    inference_app_run(PARAGRAPH_TYPE)
    if len(filtered_paragraphs) == 0:
        return filtered_paragraphs
    llm_results = generate_pipeline([p["text"] for p in filtered_paragraphs], models["inference_url"], models["inference_instruction"])
    if (len(llm_results) != len(filtered_paragraphs)):
        logger.debug(f'ERROR getting {len(llm_results)} results but had {len(filtered_paragraphs)} inputs paragraphs')
        assert(len(llm_results) != len(filtered_paragraphs))
    publi_id_map = {}
    for ix, p in enumerate(filtered_paragraphs):
        p[f'llm_{PARAGRAPH_TYPE}'] = llm_results[ix]
        publi_id = p['publication_id']
        if publi_id not in publi_id_map:
            publi_id_map[publi_id] = []
        publi_id_map[publi_id].append(p)
    for publi_id in publi_id_map:
        filename = get_filename(publi_id, PARAGRAPH_TYPE, 'llm')
        df_tmp = pd.DataFrame(publi_id_map[publi_id])
        if len(df_tmp):
            df_tmp.to_json(filename, orient='records', lines=True)
    return filtered_paragraphs
