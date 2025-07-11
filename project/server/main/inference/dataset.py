import pickle
import re
import os
import fasttext
from project.server.main.paragraphs.dataset import is_dataset
from project.server.main.utils import download_file, clean_dir
from project.server.main.logger import get_logger
logger = get_logger(__name__)

def infere_is_dataset(txt):
    p = {'text': txt}
    if is_dataset(p):
        return True
    prediction = model.predict(txt)
    proba = prediction[1][0]
    if prediction[0][0] == 'label__is_dataset' and proba > 0.5:
        return True

    return False
