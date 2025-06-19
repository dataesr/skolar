import pickle
import re
import os
import datetime
import numpy as np
import random
import pandas as pd
from project.server.main.logger import get_logger
from project.server.main.utils import download_file, clean_dir
from project.server.main.s3 import client_s3, upload_s3
from project.server.main.training.dataset import is_dataset
from project.server.main.training.software import is_software
from project.server.main.training.acknowledgement import is_acknowledgement
from project.server.main.training.clinicaltrial import is_clinicaltrial
logger = get_logger(__name__)

ALL_FIELDS = ['is_dataset', 'is_software', 'is_acknowledgement', 'is_clinicaltrial'] 
mentions_map = None

FASTTEXT_INSTALLED = False

def install_fasttext():
    global FASTTEXT_INSTALLED
    if FASTTEXT_INSTALLED:
        return
    os.system('sh /src/install_fasttext.sh')
    FASTTEXT_INSTALLED = True

def infer_type(txt):
    p_type = None
    w_s = txt.lower().replace('\n', ' ').replace('  ', ' ').replace('  ', ' ').strip().split(' ')
    ws1 = w_s[0]
    ws2 = " ".join(w_s[0:2])
    ws3 = " ".join(w_s[0:3])
    if ws1 in ['figure', 'fig', 'figura']:
        p_type = 'figure'
    elif ws1 in ['table', 'tableau']:
        p_type = 'table'
    elif ws1 in ['appendix']:
        p_type = 'appendix'
    elif ws1 in ['acknowledgements', 'acknowledgments', 'remerciements', 'agradecimientos']:
        p_type = 'acknowledgement'
    elif ws2 in ["déclaration d'intérêts"]:
        p_type = 'coi'
    elif ws2 in ['data availability']:
        p_type = 'availability'
    elif ws3 in ['conflict of interest', 'déclaration de liens',
                     'declaration of competing', 'conflict of interest']:
        p_type = 'coi'
    elif len(w_s) > 1000:
        p_type = 'too_long'
    return p_type

def build_training_all():
    data = client_s3.list_objects_v2(Bucket='skolar', Prefix='training/raw_paragraphs_from_grobid')
    existing_files = [e['Key'].split('/')[-1] for e in data['Contents']]
    for input_filename in existing_files:
        build_training(input_filename)
    aggregate_training_parts()


def build_training(input_filename, fields = ALL_FIELDS):
    logger.debug(f'reading {input_filename}')
    data_url = f'https://skolar.s3.eu-west-par.io.cloud.ovh.net/training/raw_paragraphs_from_grobid/{input_filename}'
    local_filename = f'/data/training/raw_paragraphs_from_grobid/{input_filename}'
    os.system(f'mkdir -p /data/training/raw_paragraphs_from_grobid')
    if not os.path.isfile(local_filename):
        download_file(data_url, local_filename)
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
        d['rdm'] = 100 * random.random()
        if 'is_dataset' in fields:
            d['is_dataset'] = is_dataset(d)
        if 'is_software' in fields:
            d['is_software'] = is_software(d)
        if 'is_acknowledgement' in fields:
            d['is_acknowledgement'] = is_acknowledgement(d)
        if 'is_clinicaltrial' in fields:
            d['is_clinicaltrial'] = is_clinicaltrial(d)
    df_training = pd.DataFrame(clean_data)
    for field in fields:
        save_training_validation(field, df_training , part_name)


def aggregate_training_parts(fields = ALL_FIELDS):
    for field in fields:
        logger.debug(f'aggregating {field} data')
        for t in ['validation', 'training', 'test']:
            logger.debug(f'aggregating {field} {t} data')
            os.system(f'rm -rf /data/training/{field}/{t}_global_{field}.txt')
            for f in os.listdir(f'/data/training/{field}/parts/{t}'):
                cmd = f'cat /data/training/{field}/parts/{t}/{f} >> /data/training/{field}/{t}_global_{field}.txt'
                os.system(cmd)

def fasttext_calibration(fields = ALL_FIELDS):
    install_fasttext()
    os.system(f'mkdir -p /data/models')
    size = "1M"
    for field in fields:
        os.system(f'mkdir -p /data/models/{field}')
        cmd_train = f"/src/fastText/fasttext supervised -input /data/training/{field}/training_global_{field}.txt -output /data/models/{field}/model_{field}_{size} -autotune-validation /data/training/{field}/validation_global_{field}.txt -autotune-duration 1200 -autotune-modelsize {size}"
        logger.debug(cmd_train)
        os.system(cmd_train)
        cmd_test = f"/src/fastText/fasttext test /data/models/{field}/model_{field}_{size}.ftz /data/training/{field}/test_global_{field}.txt -1 0.5"
        logger.debug(cmd_test)
        os.system(cmd_test)
        upload_s3('skolar', f'/data/models/{field}/model_{field}_{size}.ftz', f'models/{field}/model_{field}_{size}.ftz', True)
        upload_s3('skolar', f'/data/models/{field}/model_{field}_{size}.vec', f'models/{field}/model_{field}_{size}.vec', True)

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
    logger.debug(f'save_training_validation {field} {part_name}')
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
    os.system(f'mkdir -p /data/training/{field}')
    os.system(f'mkdir -p /data/training/{field}/parts')
    os.system(f'mkdir -p /data/training/{field}/parts/training')
    os.system(f'mkdir -p /data/training/{field}/parts/validation')
    os.system(f'mkdir -p /data/training/{field}/parts/test')
    np.savetxt(f'/data/training/{field}/parts/training/training_{part_name}_{field}.txt', current_training_data, fmt='%s')
    np.savetxt(f'/data/training/{field}/parts/validation/validation_{part_name}_{field}.txt', current_validation_data, fmt='%s')
    np.savetxt(f'/data/training/{field}/parts/test/test_{part_name}_{field}.txt', current_validation_data, fmt='%s')
