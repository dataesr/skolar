import pandas as pd
from datasets import load_dataset
from project.server.main.inference.acknowledgement import infere_is_acknowledgement
from project.server.main.inference.dataset import infere_is_dataset
from project.server.main.inference.software import infere_is_software
from project.server.main.utils import get_models
from project.server.main.logger import get_logger
logger = get_logger(__name__)

def bench_cdl():
    dataset_funding = load_dataset("cometadata/preprint-funding")
    dataset_das = load_dataset("DataSeer/das-extraction-raw-labels")
    datasets = {}
    datasets['acknowledgement'] = dataset_funding
    datasets['dataset'] = dataset_das
    data = []
    for dataset_key in datasets:
        for s in ['train', 'test']:
            for example in datasets[dataset_key][s]:
                elt = {'dataset_key': dataset_key}
                if dataset_key == 'acknowledgement':
                    elt.update({'doi': example['doi'], 'id': 'doi'+example['doi'], 'text': example['funding_statement'].replace('\n', ' ')})
                if dataset_key == 'dataset':
                    if isinstance(example.get('filename'), str) and isinstance(example.get('gt_das'), str) and len(example['gt_das'])>5:
                        doi = '.'.join(example.get('filename').split('.')[0:-1])
                        elt.update({'doi': doi, 'id': 'doi'+doi, 'text': example['gt_das'].replace('\n', ' ')})
                if len(elt)>1:
                    data.append(elt)
    models = {}
    for model_type in ['acknowledgement', 'dataset', 'software']:
        models[model_type] = get_models(model_type)
    for elt in data:
        elt[f'is_acknowledgement'] = infere_is_acknowledgement(elt, models['acknowledgement']['fasttext_model'])
        elt[f'is_dataset'] = infere_is_dataset(elt, models['dataset']['fasttext_model'])
        elt[f'is_software'] = infere_is_software(elt, models['software']['fasttext_model'])
    return pd.DataFrame(data)
