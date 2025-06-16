import pickle
import re
import os
from project.server.main.logger import get_logger
logger = get_logger(__name__)

def is_forge_simple(s):
    for f in ['github.', 'gitlab.', 'npmjs.', 'bitbucket.', 'pypi']:
        if f in s.lower():
            return True

def is_software(p):
    txt = p['text']
    score = 0
    evidences = []
    if 'code' in txt.lower() and p.get('type') in ['availability']:
        score += 2
        evidences.append('availability')
    if (p.get('software') is True):
        for f in ['code ', 'software', 'script ', 'scripts ', 'package ', 'sas ',
                 'logiciel', 'spss', 'program ', 'linux',
                 'matlab', 'python', 'javascript', 'module', 'gurobi',
                 'julia', 'language']:
            if f in txt.lower():
                score += 2
                evidences.append('softcite'+f)
    if is_forge_simple(txt):
        score += 2
        evidences.append('forge')
    if score > 1:
        return True
    return False
