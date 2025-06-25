import pickle
import re
import os
import fasttext
from project.server.main.paragraphs.acknowledgement import is_acknowledgement
from project.server.main.inference.predict import predict
from project.server.main.utils import download_file, clean_dir, get_models
from project.server.main.logger import get_logger
logger = get_logger(__name__)

PARAGRAPH_TYPE = 'acknowledgement'

models = get_models(PARAGRAPH_TYPE)

def infere_is_acknowledgement(paragraph, fasttext_model):
    if is_acknowledgement(paragraph):
        return True
    txt = paragraph['text']
    prediction = fasttextmodel.predict(txt)
    proba = prediction[1][0]
    if prediction[0][0] == f'label__is_{PARAGRAPH_TYPE}' and proba > 0.5:
        return True
    return False

def detect_acknowledgement(paragraphs):
    global models
    filtered_paragraphs = []
    for paragraph in paragraphs:
        if infere_is_acknowledgement(paragraph, models['fasttext_model']):
            filtered_paragraphs.append(paragraph)
    llm_results = predict([p['text'] for p in filtered_paragraphs], models['inference_url'], models['instruction'])
    assert(len(llm_results) == len(filtered_paragraphs))
    for ix, p in enumerate(filtered_paragraphs):
        p['llm_result'] = llm_results[ix]
    return filtered_paragraphs

