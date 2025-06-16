import pickle
import re
import os
from project.server.main.logger import get_logger
logger = get_logger(__name__)

def is_clinicaltrial(p):
    txt = p['text']
    score = 0
    evidences = []
    for f in ['clinicaltrials', 'eudract', 'nct id', 
            'nct0', 'nct1', 'nct2', 'nct3',
            'nct:0', 'nct:1', 'nct:2', 'nct:3',
            'nct: 0', 'nct: 1', 'nct: 2', 'nct: 3'
            ]:
            if f in txt.lower():
                score += 2
                evidences.append(f)
    if score > 1:
        return True
    return False
