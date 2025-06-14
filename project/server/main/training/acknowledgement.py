import pickle
import re
import os
from project.server.main.logger import get_logger
logger = get_logger(__name__)

ack_evidence_score = {
    'anr-': 10,
    'anr grant': 2,
    'acknowledgments': 0.5,
    'funding and support': 1,
    "supported by": 0.5,
    'funding was provided': 2,
    'funding provided': 1,
    'funded by': 1,
    'research grant': 1,
    'financement': 0.5,
    'financiación': 1,
    "agradecimientos": 0.5,
    "remerciements": 0.5,
    'financial support': 1,
    'fellowship': 0.5,
    'eu project': 1,
    'h2020': 1,
    'erc grant': 2,
    'fp7': 1,
    'horizon europe': 1,
    'casdar': 1,
    'financial assistance': 1,
    'phrc': 0.5,
    'labex': 1,
    "investissements d'avenir": 1,
    'beca ': 0.5,
    "fue subvencionada": 1,
    "du soutien financier": 1,
    "fue financiada": 1,
    "equipex": 1
}

def is_acknowledgement(p):
    txt = p['text']
    if p.get('type') in ['acknowledgement', 'funding', 'coi']:
        return True
    if d.get('type') is None:
        if len(txt) > 2500:
            return False
    for f in ['acknowled', 'remerciem', 'agradeci']:
        if txt.lower().strip().startswith(f):
            return True
    score = 0
    evidences = []
    for f in ack_evidence_score.keys():
        if f in txt.lower():
            score += ack_evidence_score[f]
            evidences.append(f)
    if score > 1:
        return True
    return False
