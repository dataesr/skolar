import pickle
import re
import os
import fasttext
from project.server.main.paragraphs.software import is_software
from project.server.main.utils import download_file, clean_dir
from project.server.main.logger import get_logger
logger = get_logger(__name__)

def infere_is_software(p, model):
    if is_software(p):
        return True
    prediction = model.predict(p['text'])
    proba = prediction[1][0]
    if prediction[0][0] == '__label__is_software' and proba > 0.5:
        return True

    return False
