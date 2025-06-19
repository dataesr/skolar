import pickle
import re
import os
from project.server.main.logger import get_logger
logger = get_logger(__name__)

data_evidence_score = {
    "accession number": 2,
    "accession code": 2,
    "access number": 2,
    "accession id": 2,
    "database": 2,
    "data bank":2,
    "databank": 2,
    "dataverse": 2,
    "dataset": 2,
    "base de donnée": 2,
    "bases de donnée": 2,
    "genbank": 2,
    "pdb id": 2,
    "pdb:": 2,
    "pdb code": 2,
    "pdb access": 2,
    "pdb entr": 2,
    "dryad": 2,
    "zenodo": 2,
    "pangaea": 2,
    "f1000research": 2,
    "6073/pasta": 2
}

def has_gse(text):
    for n in range(0, 10):
        if f'GSE{n}' in text:
            return True
    return False

def has_arrayexpress(text):
    for f in ['-MEXP', '-MTAB', '-GEOD', 'CHEMBL', 'EMPIAR', "PRJNA", "EPI_ISL", "SAMN0", "SAMN1"]:
        if f in text:
            return True
    return False


def is_dataset(p):
    txt = p['text']
    score = 0
    evidences = []
    if ('data ' in txt.lower()):
        if p.get('type') in ['availability']:
            score += 2
            evidences.append('availability')
        if (p.get('dataset-name') is True) or (p.get('dataset-implicit') is True):
            score += 2
            evidences.append('datastet')
    for f in data_evidence_score.keys():
        if f in txt.lower():
            score += data_evidence_score[f]
            evidences.append(f)
    if has_gse(txt):
        score += 2
        evidences.append('GSE')
    if has_arrayexpress(txt):
        score += 2
        evidences.append('arrayexpress')
    if score > 1:
        return True
    return False
