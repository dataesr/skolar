import os
import time
import pandas as pd
import markdown_to_json
from pathlib import Path
from project.server.main.utils import chunks
from project.server.main.logger import get_logger

logger = get_logger(__name__)


def detect_md(output_llm, uid):
    parsed = {}
    output_llm = output_llm.replace("\n**", "\n\n**")  # fix parsing errors
    for e in markdown_to_json.dictify(output_llm)["root"]:
        if e[0:2] == "**":
            current_section = e.replace("**", "").strip().lower()
            parsed[current_section] = []
            current_elt = {}
        else:
            if not (isinstance(e, list)):
                # print(e)
                continue  # skip not markdown responses
            for ix, k in enumerate(e):
                if isinstance(k, str):
                    if current_elt:
                        parsed[current_section].append(current_elt)
                    current_elt = {}
                elif isinstance(k, list):
                    # print(k)
                    k_flat = []
                    for s in k:
                        if isinstance(s, list):
                            if len(s) == 0:
                                continue
                            elif len(s) == 1:
                                k_flat.append(s[0])
                            else:
                                k_flat += s
                        else:
                            k_flat.append(s)
                    for s in k_flat:
                        if ":" not in s:
                            #logger.debug(f'strange line {s} for {uid}')
                            continue
                        s_split = s.split(":")
                        field = s_split[0].replace(" ", "_").lower()
                        current_elt[field] = s_split[1].strip()
                        if current_elt[field] in ["[not provided]", "[not specified]"]:
                            del current_elt[field]
                if ix == len(e) - 1 and current_elt:
                    parsed[current_section].append(current_elt)
    return parsed

def parse_all():
    g = os.listdir('/data/llm/acknowledgement/global')
    dfs=[]
    for f in g:
        if 'global' not in f:
            print(f)
            df = pd.read_json(f'/data/llm/acknowledgement/global/{f}', lines=True, orient='records')
            dfs.append(df)
    pd.concat(dfs).to_json('/data/llm/acknowledgement/global/global.jsonl.gz', lines=True, orient='records')

    for x in os.listdir('/data/llm/acknowledgement'):
        parse_files(x)

def parse_files(prefix):
    root = f'/data/llm/acknowledgement/{prefix}'
    logger.debug(root)
    root_str = root.replace('/', '_')
    assert(' ' not in root_str)
    root_path = Path(root)
    list_path = chunks(list(root_path.rglob("*.jsonl")), 1000)
    cx = 0
    dfs=[]
    for c in list_path:
        cx += 1
        tmp_file = f'/data/tmp_{root_str}_{cx}.tmp.jsonl'
        list_path_str = ' '.join([str(p) for p in c])
        logger.debug(f'concat {len(c)} jsonl files into {tmp_file}')
        os.system(f'cat {list_path_str} > {tmp_file}')
        time.sleep(5)
        try:
            current_data = pd.read_json(tmp_file, lines=True).to_dict(orient='records')
        except:
            logger.debug(f'error !!!! for {prefix}')
        for e in current_data:
            if e.get('llm_acknowledgement'):
                try:
                    infos = detect_md(e.get('llm_acknowledgement'), e['uid'])
                    e['structured_ackowledgement'] = infos
                except:
                    #logger.debug(f'detect_md failed for {e}')
                    continue
        if current_data:
            dfs.append(pd.DataFrame(current_data))
    os.system('mkdir -p /data/llm/acknowledgement/global')
    if dfs:
        pd.concat(dfs).to_json(f'/data/llm/acknowledgement/global/{prefix}.jsonl', lines=True, orient='records')
