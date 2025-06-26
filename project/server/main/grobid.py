import requests
import os
import hashlib
import bs4
from bs4 import BeautifulSoup
from project.server.main.logger import get_logger
logger = get_logger(__name__)

TYPES = ['introduction', 'method', 'result', 'conclusion', 'acknowledgement', 'funding', 'discussion']

MIN_WORD_LENGTH = 4
MIN_NB_WORS = 10

def run_grobid(pdf_file, output_file):
    assert(os.path.isfile(pdf_file))
    if os.path.isfile(output_file):
        print(f'already done {output_file}')
        return output_file
    grobid_url = 'http://grobid:8070/api/processFulltextDocument'
    file_handle =  open(pdf_file, 'rb')
    files = {'input': file_handle}
    logger.debug(f'grobid for file {pdf_file} ...')
    data = {'consolidatFunders': 1, 'includeRawAffiliations': 1}
    res = requests.post(grobid_url, files=files, data=data)
    file_handle.close()
    out_handle= open(output_file, 'w')
    out_handle.write(res.text)
    out_handle.close()
    logger.debug(f'{output_file} written.')
    return output_file

def parse_grobid(xml_path, publication_id):
    xml_handler = open(xml_path, 'r')
    soup = BeautifulSoup(xml_handler, 'html.parser')
    xml_handler.close()
    uid = xml_path.split('/')[-1].split('.')[0]

    if soup is None:
        return {}
    paragraphs = []
    known_hash = set()

    abstract_elt = soup.find('abstract')
    if abstract_elt:
        current_text = fix_text(abstract_elt)
        paragraphs, known_hash = add_text(current_text, 'abstract', paragraphs, known_hash, uid, False)

    current_type = None
    for d in soup.find_all('div'):
        skip = False
        current_text = fix_text(d)
        current_type_from_grobid = d.attrs.get('type')
        if current_type_from_grobid is not None:
            current_type = current_type_from_grobid
        elif len(current_text.split(' ')) <= 2:
            for t in ['method', 'result', 'discussion']:
                if t in current_text.lower():
                    current_type = current_text.lower()
                    skip = True
        paragraphs, known_hash = add_text(current_text, current_type, paragraphs, known_hash, uid, skip)

    body = soup.find('body')
    if body:
        for c in list(body.children):
            verbose = False
            skip = False
            if isinstance(c, bs4.element.NavigableString):
                continue
            else:
                current_text = fix_text(c)
                paragraphs, known_hash = add_text(current_text, None, paragraphs, known_hash, uid, skip, verbose)
   
    for p in paragraphs:
        p['uid'] = uid
        p['publication_id'] = publication_id
    return paragraphs

def decompose_text(text):
    ans = []
    subtexts =  [t.strip() for t in text.split('\n')]
    for subt in subtexts:
        if len(subt) < MIN_WORD_LENGTH:
            continue
        sentences = subt.split('.')
        current_text = ""
        for s in sentences:
            if s.strip():
                current_text += s.strip()+". "
            if len(current_text) > 800:
                ans.append(current_text)
                current_text = ""
        if current_text:
            ans.append(current_text)
    return ans

def fix_text(current_elt):
    links = []
    refs = current_elt.find_all('ref')
    for ref in refs:
        if 'target' in ref.attrs:
            if ref.attrs['target'].startswith('#'):
                continue
            links.append(ref.attrs['target'].replace(' ', ''))
    text = current_elt.get_text(' ').strip()
    for let in ['a', 'e', 'i', 'o', 'u', "c"]:
        for acc in ["ˆ", "´", "¸", "`"]:
            text = text.replace(f"{let} {acc}", let)
    for link in links:
        text += f' link = {link} ; '
    return text


def add_text(current_text, current_type, paragraphs, known_hash, uid, skip, verbose=False):
    if len(current_text.strip()) <= 1:
        return paragraphs, known_hash
    current_hash = hashlib.md5(current_text.encode('utf-8')).hexdigest()
    if current_hash not in known_hash:
        first_word = current_text.strip().split(' ')[0].lower()
        for t in TYPES + ['fig.']:
            if t in first_word:
                current_type = t
                break
        type_to_use = current_type
        if skip:
            type_to_use= 'skip'
        current_paragraph_id = uid + '--' + str(len(paragraphs))
        current_paragraph_hash = hashlib.md5(current_paragraph_id.encode('utf-8')).hexdigest()
        for subtext in decompose_text(current_text):
            paragraphs.append({'text': subtext, 'type': type_to_use, 'hash': current_paragraph_hash})
            known_hash.add(current_hash)
        if verbose:
            print(f'adding new text {current_text[0:100]}')
    return paragraphs, known_hash
