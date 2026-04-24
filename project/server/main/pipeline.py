import pandas as pd
import json
import pymongo
import os
import pickle
from project.server.main.harvester.test import process_publication
from project.server.main.grobid import parse_grobid
from project.server.main.paragraphs.acknowledgement.llm import acknowledgement_llm_completions
from project.server.main.paragraphs.acknowledgement.filter import acknowledgement_filter
from project.server.main.paragraphs.dataset.llm import dataset_llm_completions
from project.server.main.paragraphs.dataset.filter import dataset_filter
#from project.server.main.paragraphs.software.llm import software_llm_completions
from project.server.main.paragraphs.software.filter import software_filter
#from project.server.main.paragraphs.clinicaltrial.llm import clinicaltrial_llm_completions
from project.server.main.paragraphs.clinicaltrial.filter import clinicaltrial_filter
from project.server.main.utils import (
    id_to_string,
    cp_folder_local_s3,
    sync_local_to_s3,
    gzip_all_files_in_dir,
    get_elt_id,
    get_filename,
    is_dowloaded,
    has_acknowledgement,
    read_jsonl,
    to_jsonl,
)
from project.server.main.mongo import get_oa
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def get_already_computed():
    all_ids = set()
    for f in os.listdir('/data/acknowledgement'):
        logger.debug(f)
        df = pd.read_json(f'/data/acknowledgement/{f}', lines=True)
        all_ids.update(df.publication_id.to_list())
    logger.debug(f'{len(all_ids)} ids already ok')
    pickle.dump(all_ids, open('/data/computed_ids.pkl', 'wb'))

ALREADY_COMPUTED_IDS = pickle.load(open('/data/computed_ids.pkl', 'rb'))

FILTER_FN = {
    "acknowledgement": acknowledgement_filter,
    "dataset": dataset_filter,
    "software": software_filter,
    "clinicaltrial": clinicaltrial_filter,
}

LLM_COMPLETIONS_FN = {
    "acknowledgement": acknowledgement_llm_completions,
    "dataset": dataset_llm_completions,
 #   "software": software_llm_completions,
 #   "clinicaltrial": clinicaltrial_llm_completions,
}

# def get_already_done(input_dir, reset=True):
#    assert(input_dir in ['grobid', 'publisher-xml'])
#    cache_filename = f'/data/already_done_{input_dir}.pkl'
#    if reset is False:
#        done = pickle.load(open(cache_filename, 'rb'))
#        logger.debug(f'{len(done)} files already done for {input_dir}')
#        return done
#    done = []
#    for e in os.walk(f'/data/{input_dir}'):
#        if e[2]:
#            done += e[2]
#    done = set(done)
#    logger.debug(f'recomputed: {len(done)} files already done for {input_dir}')
#    pickle.dump(done, open(cache_filename, 'wb'))
#    return done

def enrich_with_metadata(df):
    if "doi" not in df.columns:
        df["doi"] = df["id"].apply(lambda x: x.replace("doi10", "10"))
    df['doi'] = df['doi'].apply(lambda x:x.lower().strip())
    dois = [d for d in df.doi.unique().tolist() if isinstance(d, str)]
    extra_metadata = get_oa(dois) # get info from unpaywall db
    extra_dict = {}
    for e in extra_metadata:
        extra_dict[e['doi']] = e
    new_data = []
    for e in df.to_dict(orient='records'):
        if e['doi'] in extra_dict:
            e.update(extra_dict[e['doi']])
        new_data.append(e)
    return new_data


def concat_files(elts, paragraph_type="acknowledgement", from_dir="llm"):
    all_data = []
    for elt in elts:
        filename = get_filename(elt["id"], paragraph_type, from_dir)
        try:
            current_data = pd.read_json(filename, lines=True).to_dict(orient='records')
            all_data += current_data
        except:
            pass
    logger.debug(f"{len(all_data)} publis with {paragraph_type} data collected in the chunk")
    return all_data


def download_and_grobid(elts, worker_idx, use_cache=True):
    xml_paths = []
    for elt in elts:
        if elt.get('hal_docType') in ['VIDEO', 'video']:
            logger.debug(f"skip video {elt['id']}")
            continue
        xml_path = process_publication(elt = elt, worker_idx = worker_idx, use_cache = use_cache) # download + run_grobid
        if xml_path:
            xml_paths.append(xml_path)
    gzip_all_files_in_dir(f'/data/pdf_{worker_idx}')
    cp_folder_local_s3(f'/data/pdf_{worker_idx}', 'pdf')
    os.system(f'rm -rf /data/pdf_{worker_idx}')
    logger.debug(f'{len(xml_paths)} xmls extracted')
    return 

def parse_paragraphs(elts, worker_idx, paragraph_type, use_cache=True, use_llm=True):
    paragraphs = []
    xml_paths = []
    logger.debug(f"{paragraph_type}: start going through paths for {len(elts)} elts")

    for elt in elts:
        elt_id = get_elt_id(elt)
        xml_path = get_filename(elt_id, 'grobid')
        if os.path.isfile(xml_path):
            xml_paths.append(xml_path)
        else:
            xml_path = get_filename(elt_id, 'publisher-xml')
            if os.path.isfile(xml_path):
                xml_paths.append(xml_path)
    logger.debug(f"{paragraph_type}: {len(xml_paths)} / {len(elts)} files have an XML")

    new_parsing, new_filtering, llm_call = 0, 0, 0
    already_parsed, already_filtered, already_llm = 0, 0, 0
    llm_res = []

    for xml_path in xml_paths:
        uid = xml_path.split('/')[-1].split('.')[0]
        elt_id = id_to_string(uid)
        filename_paragraph = get_filename(elt_id, f"all_paragraphs")
        filename_filter = get_filename(elt_id, paragraph_type, "filter")
        filename_llm = get_filename(elt_id, paragraph_type, "llm")
        is_parsed, is_filtered, is_analyzed = False, False, False

        if use_cache and os.path.isfile(filename_paragraph):
            already_parsed += 1
            is_parsed = True
        
        if use_cache and os.path.isfile(filename_filter):
            already_filtered += 1
            is_filtered = True

        if use_cache and os.path.isfile(filename_llm):
            already_llm += 1
            is_analyzed = True

        if (use_cache is False) or (is_parsed is False):
            new_parsing += 1
            paragraphs = parse_grobid(xml_path, elt_id, worker_idx)
        else:
            paragraphs = read_json(filename_paragraph)

        if (use_cache is False) or (is_filtered is False):
            if paragraph_type in FILTER_FN:
                filtered_paragraphs = FILTER_FN[paragraph_type](elt_id, paragraphs)
                new_filtering += 1
            else:
                logger.error(f"{paragraph_type}: filter function not found")
        else:
            filtered_paragraphs = read_jsonl(filename_filter)

        if use_llm:
            if (use_cache is False) or (is_analyzed is False):
                try:
                    llm_res += LLM_COMPLETIONS_FN[paragraph_type](elt_id, filtered_paragraphs)
                    llm_call += 1
                except Exception as error:
                    logger.error(f"{paragraph_type}: error for {elt_id}: {error}")
            else:
                llm_res += read_jsonl(filename_llm)
    logger.debug(f"{paragraph_type}: already_parsed: {already_parsed}, already_filtered: {already_filtered}, already_llm: {already_llm}")
    logger.debug(f"{paragraph_type}: new parsed: {new_parsing}, new_filtered: {new_filtering}, LLM calls: {llm_call}")
    logger.debug(f'{len(xml_paths)} xmls extracted')
    return llm_res


def test():
    input_file = "/src/validation/validation_from_florian_naudet_constant_vinatier.csv"
    df = pd.read_csv(input_file)
    elts = df.to_dict(orient="records")
    paragraphs = parse_paragraphs(elts=elts, worker_idx=1, paragraph_type="acknowledgement", use_cache=True, use_llm=True)
    return paragraphs


# def get_from_live_unpaywall(doi):
#     url = f'https://api.unpaywall.org/v2/{doi}?email=unpaywall_01@example.com'
#     return requests.get(url).json()

# def import_acknowledgments():
#     myclient = pymongo.MongoClient('mongodb://mongo:27017/')
#     mydb = myclient['scanr']
#     collection_name = 'acknowledgments_v2'
#     mydb[collection_name].drop()
#     for f in os.listdir('/data/acknowledgement'):
#         current_file = f'/data/acknowledgement/{f}'
#         mongoimport = f'mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/scanr --file {current_file}' \
#                   f' --collection {collection_name}'
#         os.system(mongoimport)
#     mycol = mydb[collection_name]
#     for f in ['publication_id']:
#         mycol.create_index(f)
#     myclient.close()


def validation():
    input_file = "/src/validation/validation_from_florian_naudet_constant_vinatier.csv"
    args = {"paragraph_types": ["acknowledgement"], "download": True, "parse": True, "detect": True}
    run_from_file(input_file=input_file, args=args, worker_idx=1)
    df = pd.read_csv(input_file)
    data = []
    for e in df.to_dict(orient="records"):
        e["elt_id"] = "doi" + e["doi"].lower().strip()
        e["is_downloaded"] = is_dowloaded(e["elt_id"])
        e["has_acknowledgement"] = has_acknowledgement(e["elt_id"])
        data.append(e)
    pd.DataFrame(data).to_csv("/data/validation.csv", index=False)


def run_list_publi(publi_ids, paragraph_type, use_cache_grobid, use_cache_paragraph, use_llm):
    c = pd.DataFrame({"id": publi_ids})
    c["doi"] = c["id"].apply(lambda x: x.replace("doi10", "10"))
    elts = enrich_with_metadata(c)
    download_and_grobid(elts=elts, worker_idx=1, use_cache=use_cache_grobid)
    parse_paragraphs(elts, worker_idx=1, paragraph_type=paragraph_type, use_cache=use_cache_paragraph, use_llm=use_llm)
    # filename = get_filename(elts[0]['id'], 'acknowledgement', 'llm')


def run_from_file(input_file, args, worker_idx):
    os.system(f"mkdir -p /data/pdf_{worker_idx}")
    os.system(f"mkdir -p /data/all_paragraphs")
    download = args.get("download", False)
    parse = args.get("parse", False)
    use_cache_grobid = args.get("use_cache_grobid", True)
    use_cache_paragraph = args.get("use_cache_paragraph", True)
    use_llm = args.get("use_llm", False)
    concat = args.get("concat", False)
    chunksize = args.get("chunksize", 100)
    early_stop = args.get("early_stop", False)
    paragraph_types = args.get("paragraph_types", ["acknowledgement"])
    logger.info(f"Start run with args: {args}")
    if ("jsonl" in input_file) or ("chunk_bso" in input_file):
        df = pd.read_json(input_file, lines=True, chunksize=chunksize)
    elif "csv" in input_file:
        df = pd.read_csv(input_file, chunksize=chunksize)
    chunk_idx = 0
    files_to_concat = {key: [] for key in paragraph_types}
    for c in df:
        chunk_idx += 1
        logger.debug(f"NEW CHUNK {chunk_idx}")
        cols = list(c.columns)
        elts = []
        if ("oa_details" not in cols) and ("oa_locations" not in cols):
            elts = enrich_with_metadata(c)
        else:
            elts = c.to_dict(orient="records")
        logger.debug(f"len elts = {len(elts)}")
        if download or parse:
            elts = [e for e in elts if e["id"] not in ALREADY_COMPUTED_IDS]
            logger.debug(f"len elts = {len(elts)} after removing ALREADY_COMPUTED_IDS")
        if download:
            download_and_grobid(elts, worker_idx, use_cache_grobid)
        for paragraph_type in paragraph_types:
            if parse:
                parse_paragraphs(elts, worker_idx, paragraph_type, use_cache_paragraph, use_llm)
            if concat:
                concat_from_dir = "llm" if use_llm else "filter"
                files_to_concat[paragraph_type] += concat_files(elts, paragraph_type, concat_from_dir)
        if early_stop:
            break
    if concat:
        current_file = input_file.split("/")[-1].split(".")[0]
        for paragraph_type in paragraph_types:
            output_file = f"/data/{paragraph_type.lower()}/{current_file}.jsonl"
            os.system(f"rm -rf {output_file}")
            logger.debug(f"{paragraph_type}: writing {len(files_to_concat[paragraph_type])} elts into {output_file}")
            if files_to_concat[paragraph_type]:
                os.makedirs(os.path.dirname(output_file) or ".", exist_ok=True)  # fix no found directory
                to_jsonl(files_to_concat[paragraph_type], f"{output_file}")
