import pickle
import re
import os
import datetime
import numpy as np
import random
import pandas as pd
from project.server.main.logger import get_logger
from project.server.main.utils import download_file, clean_dir
from project.server.main.training.dataset import is_dataset
from project.server.main.training.software import is_software
from project.server.main.training.acknowledgement import is_acknowledgement
logger = get_logger(__name__)

mentions_map = None

def build_training(input_filename, fields = ['is_dataset']):
    logger.debug(f'reading {input_filename}')
    data = pd.read_json(input_filename, lines=True).to_dict(orient='records')
    part_name = input_filename.split('/')[-1].split('.')[0]
    logger.debug(f'{len(data)} rows read for {part_name}')
    data = tag_mentions(data)
    for d in data:
        w_s = d['text'].lower().replace('\n', ' ').replace('  ', ' ').replace('  ', ' ').strip().split(' ')
        ws1 = w_s[0]
        ws2 = " ".join(w_s[0:2])
        ws3 = " ".join(w_s[0:3])
        if ws1 in ['figure', 'fig', 'figura']:
            d['type'] = 'figure'
        elif ws1 in ['table', 'tableau']:
            d['type'] = 'table'
        elif ws1 in ['appendix']:
            d['type'] = 'appendix'
        elif ws1 in ['acknowledgements', 'acknowledgments', 'remerciements', 'agradecimientos']:
            d['type'] = 'acknowledgement'
        elif ws2 in ["déclaration d'intérêts"]:
            d['type'] = 'coi'
        elif ws2 in ['data availability']:
            d['type'] = 'availability'
        elif ws3 in ['conflict of interest', 'déclaration de liens',
                     'declaration of competing', 'conflict of interest']:
            d['type'] = 'coi'
        elif len(w_s) > 1000:
            d['type'] = 'too_long'

    clean_data = [d for d in data if d.get('type') not in ['figure', 'too_long', 'table', 'annex', 'references', 'appendix']]
    for d in clean_data:
        d['rdm'] = 100 * random.random()
        if 'is_dataset' in fields:
            d['is_dataset'] = is_dataset(d)
        if 'is_software' in fields:
            d['is_software'] = is_software(d)
        if 'is_acknowledgement' in fields:
            d['is_acknowledgement'] = is_acknowledgement(d)
    df_training = pd.DataFrame(clean_data)
    for field in fields:
        save_training_validation(field, df_training , part_name)


def aggregate_training_parts(fields):
    for field in fields:
        for t in ['validation', 'training', 'test']:
            os.system(f'rm -rf /data/training/{field}/{t}_global_{field}.txt')
            for f in os.listdir(f'/data/training/{field}/parts/{t}'):
                cmd = f'cat /data/training/{field}/parts/{t}/{f} >> /data/training/{field}/{t}_global_{field}.txt'
                os.system(cmd)

def get_mentions():
    logger.debug('reading mentions')
    global mentions_map
    if mentions_map:
        return mentions_map
    mentions_url = 'https://skolar.s3.eu-west-par.io.cloud.ovh.net/training/mentions_from_bso/sampled_mentions.pkl'
    os.system('mkdir -p /data/training && mkdir -p /data/training/mentions_from_bso')
    if not os.path.isfile('/data/training/mentions_from_bso/sampled_mentions.pkl'):
        download_file(mentions_url, '/data/training/mentions_from_bso/sampled_mentions.pkl')
    mentions = pickle.load(open('/data/training/mentions_from_bso/sampled_mentions.pkl', 'rb'))
    mentions_map = {}
    for m in mentions:
        if m['doi'] not in mentions_map:
            mentions_map[m['doi']] = []
        mentions_map[m['doi']].append(m)
    return mentions_map


def tag_mentions(data):
    logger.debug(f'adding mentions info to the {len(data)} samples')
    global mentions_map
    if mentions_map is None:
        mentions_map = get_mentions()
    for ix, d in enumerate(data):
        current_txt = d['text']
        if d['doi'] not in mentions_map:
            continue
        for m in mentions_map[d['doi']]:
            if 'context' not in m:
                continue
            if m['context'].replace(' ', '').replace('-','') in current_txt.replace(' ', '').replace('-',''):
                d[m['type']] = True
    return data

def save_training_validation(field, df_training, part_name):
    random.seed(1987)
    validation_percentage = 0.1
    test_percentage = 0.1
    nb_field = len(df_training[df_training[field]])
    field_proportion = nb_field / len(df_training)
    current_training_data, current_validation_data, current_test_data = [], [], []
    nb_pos, nb_neg = 0, 0
    for r in df_training.to_dict(orient='records'):
        keep_training, keep_validation, keep_test = False, False, False
        if r['rdm'] > 100 * (1 - test_percentage):
            keep_test = True
        elif r['rdm'] <= 100 * (1 - (test_percentage + validation_percentage)):
            keep_training = True
        else:
            keep_validation = True
        if r[field]:
            label = '__label__'+field
            nb_pos += 1
        else:
            label = '__label__'+field.replace('is_', 'no_')
            if random.random() > field_proportion:
                continue
            nb_neg += 1
        new_line = label + " " + r['text'].replace('\n', ' ')
        if keep_training:
            current_training_data.append(new_line)
        if keep_validation:
            current_validation_data.append(new_line)
        if keep_test:
            current_test_data.append(new_line)
    if field in ['is_software', 'is_forge']:
        for datalist in [current_training_data, current_validation_data]:
            current_len = int(max(100, 0.2 * (len(datalist)/2)))
            logger.debug(f'adding {current_len} times extra tokens for field {field}')
            for k in range(0, current_len):
                for token in ["https://github.com/", "github.", "gitlab.", "bitbucket.", "npmjs."]:
                    datalist.append(f'__label__{field} {token}')
    logger.debug(f"{field} nb_pos={nb_pos} nb_neg={nb_neg}")
    nb_test = len(current_test_data)
    nb_validation = len(current_validation_data)
    nb_training = len(current_training_data)
    logger.debug(f"{field}: nb_training={nb_training} | nb_validation={nb_validation} | nb_test={nb_test}")
    np.savetxt(f'/data/training/{field}/parts/training/training_{part_name}_{field}.txt', current_training_data, fmt='%s')
    np.savetxt(f'/data/training/{field}/parts/validation/validation_{part_name}_{field}.txt', current_validation_data, fmt='%s')
    np.savetxt(f'/data/training/{field}/parts/test/test_{part_name}_{field}.txt', current_validation_data, fmt='%s')
